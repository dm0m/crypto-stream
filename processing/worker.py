import asyncio
from functools import partial
from typing import Any, cast

import structlog
from pydantic import ValidationError
from redis import ResponseError
from redis.asyncio import Redis
from redis.typing import EncodableT, FieldT

from core.backoff import race_wait_task, retry_call
from core.log_events import LogEvent
from domain.trade import Trade
from storage.repositories import TradeRepository

# One stream entry as redis-py returns it with decode_responses=True:
# (entry_id, {"field": "value", ...}). Trades live in the "data" field as JSON.
StreamEntry = tuple[str, dict[str, str]]
# Shape of an XREADGROUP reply: one (stream_name, entries) pair per stream read.
StreamResp = list[tuple[str, list[StreamEntry]]]


class TradeWorker:
    """Consumer-group worker that moves trades from Redis Streams into Postgres."""

    STREAM = "trades.raw"
    GROUP = "trades"
    DLQ_STREAM = "trades.dlq"
    MIN_IDLE_TIME = 60000
    MAX_RETRIES = 3

    def __init__(
        self,
        *,
        consumer: str,
        recovery_consumer: str,
        redis_client: Redis,
        redis_count: int = 500,
        redis_block: int = 1000,
        trade_repository: TradeRepository,
        shutdown_event: asyncio.Event,
    ) -> None:
        """Configure a worker; nothing touches Redis or Postgres until ``run``."""
        self.CONSUMER = consumer
        self.RECOVERY_CONSUMER = recovery_consumer
        self.REDIS_COUNT = redis_count
        self.REDIS_BLOCK = redis_block
        self._redis_client: Redis = redis_client
        self._trade_repository: TradeRepository = trade_repository
        self.logger: structlog.BoundLogger = structlog.get_logger().bind(
            service="processing", group=self.GROUP, consumer=self.CONSUMER
        )
        self._shutdown_event = shutdown_event
        self.processed_count = 0

    async def ensure_group(self) -> None:
        """Create the consumer group (and the stream, if missing) idempotently.

        Raises:
            redis.ResponseError: For any failure other than the group already
                existing, e.g. the key holds a non-stream type.
        """

        async def create_group() -> None:
            try:
                await self._redis_client.xgroup_create(
                    self.STREAM, self.GROUP, mkstream=True
                )
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

        await retry_call(create_group, event=LogEvent.REDIS_RECONNECTING)

    async def _ack(self, *ids: str) -> None:
        await retry_call(
            lambda: self._redis_client.xack(self.STREAM, self.GROUP, *ids),
            event=LogEvent.REDIS_RECONNECTING,
        )

    async def _flush(self, trades: list[Trade], ids: list[str]) -> None:
        """Bulk-insert ``trades`` and then acknowledge ``ids``, in that order."""
        await retry_call(
            lambda: self._trade_repository.bulk_insert(trades),
            event=LogEvent.DB_RECONNECTING,
        )
        await self._ack(*ids)

    async def process_batch(self, resp: StreamResp) -> None:
        """Parse one XREADGROUP reply, persist its trades, and ack them.

        Raises:
            KeyError: An entry has no ``data`` field.
            pydantic.ValidationError: An entry's ``data`` is not a valid
                ``Trade`` JSON document.
        """
        trades_to_insert: list[Trade] = []
        ids: list[str] = []
        for _, entries in resp:
            for entry_id, fields in entries:
                trade = Trade.model_validate_json(fields["data"])
                ids.append(entry_id)
                trades_to_insert.append(trade)
        if trades_to_insert:
            await self._flush(trades_to_insert, ids)
            self.processed_count += len(ids)
            self.logger.info(LogEvent.PROCESSING_BATCH, read=len(ids))

    async def run(self) -> None:
        """Start the worker and block until shutdown is requested."""
        self.logger.info(LogEvent.PROCESSING_STARTED, stream=self.STREAM)
        await self.ensure_group()
        recovery_task = asyncio.create_task(self.recover_failed_msgs())
        log_dlq_task = asyncio.create_task(self.log_dlq_count())
        try:
            await self.consume()
        except asyncio.CancelledError:
            pass
        self.logger.info(LogEvent.SHUTDOWN_INITIATED)
        recovery_task.cancel()
        log_dlq_task.cancel()
        await asyncio.gather(recovery_task, return_exceptions=True)
        await asyncio.gather(log_dlq_task, return_exceptions=True)
        self.logger.info(LogEvent.SHUTDOWN_COMPLETE)

    async def log_dlq_count(self, delay: int = 30) -> None:
        while True:
            count: int = await retry_call(
                lambda: self._redis_client.xlen(self.DLQ_STREAM),
                event=LogEvent.REDIS_RECONNECTING,
            )
            if count > 0:
                self.logger.info(LogEvent.DLQ_NOT_EMPTY)
            await asyncio.sleep(delay)

    async def consume(self) -> None:
        """Main loop: read new entries from the group and process them in batches.

        Raises:
            asyncio.CancelledError: Shutdown fired during a blocking read.
            Exception: Anything ``process_batch`` or Redis raised, after
                logging.
        """
        try:
            while not self._shutdown_event.is_set():
                read_task = asyncio.create_task(
                    retry_call(
                        lambda: self._redis_client.xreadgroup(
                            self.GROUP,
                            self.CONSUMER,
                            {self.STREAM: ">"},
                            count=self.REDIS_COUNT,
                            block=self.REDIS_BLOCK,
                        ),
                        event=LogEvent.REDIS_RECONNECTING,
                    )
                )
                resp = cast(
                    StreamResp, await race_wait_task(read_task, self._shutdown_event)
                )
                if not resp:
                    continue
                await self.process_batch(resp)
        except Exception as e:
            self.logger.exception(LogEvent.PROCESSING_ERROR, error=str(e))
            raise

    async def recover_failed_msgs(self) -> None:
        """Sweep the group's pending entries once and re-process or dead-letter them."""
        start = "0-0"
        try:
            while True:
                next_id, entries, _ = await retry_call(
                    partial(
                        self._redis_client.xautoclaim,
                        self.STREAM,
                        self.GROUP,
                        self.RECOVERY_CONSUMER,
                        self.MIN_IDLE_TIME,
                        start,
                    ),
                    event=LogEvent.REDIS_RECONNECTING,
                )
                trades_to_insert: list[Trade] = []
                ids: list[str] = []
                for msg_id, fields in entries:
                    delivery_count = await self.get_delivery_count(msg_id)
                    if delivery_count >= self.MAX_RETRIES:
                        await self.send_to_dlq(msg_id, fields, delivery_count)
                        await self._ack(msg_id)
                        continue
                    trade = Trade.model_validate_json(fields["data"])
                    trades_to_insert.append(trade)
                    ids.append(msg_id)
                if trades_to_insert:
                    await self._flush(trades_to_insert, ids)
                if next_id == "0-0":
                    break
                start = next_id
        except (KeyError, ValidationError) as e:
            self.logger.exception(LogEvent.PROCESSING_ERROR, error=str(e))
        await self.prune_dead_consumers()

    async def prune_dead_consumers(self) -> None:
        """Delete consumers that are idle with nothing pending, so restarts do not pile up."""
        consumers = cast(
            list[dict[str, Any]],
            await retry_call(
                lambda: self._redis_client.xinfo_consumers(self.STREAM, self.GROUP),
                event=LogEvent.REDIS_RECONNECTING,
            ),
        )
        own = {self.CONSUMER, self.RECOVERY_CONSUMER}
        for consumer in consumers:
            name = consumer["name"]
            if (
                name in own
                or consumer["pending"] > 0
                or consumer["idle"] < self.MIN_IDLE_TIME
            ):
                continue
            await retry_call(
                partial(
                    self._redis_client.xgroup_delconsumer, self.STREAM, self.GROUP, name
                ),
                event=LogEvent.REDIS_RECONNECTING,
            )
            self.logger.info(
                LogEvent.PROCESSING_CONSUMER_PRUNED,
                pruned=name,
                idle_ms=consumer["idle"],
            )

    async def send_to_dlq(
        self, msg_id: str, fields: dict[str, str], delivery_count: int
    ) -> None:
        """Copy a poisoned entry onto ``DLQ_STREAM`` with diagnostic fields."""
        payload = cast(
            dict[FieldT, EncodableT],
            {
                **fields,
                "message_id": msg_id,
                "delivery_count": delivery_count,
                "error": "max_retries_exceeded",
            },
        )
        await retry_call(
            lambda: self._redis_client.xadd(self.DLQ_STREAM, payload),
            event=LogEvent.PROCESSING_ERROR,
        )

    async def get_delivery_count(self, msg_id: str) -> int:
        """Return how many times the group has delivered ``msg_id``."""
        pending = await retry_call(
            lambda: self._redis_client.xpending_range(
                self.STREAM, self.GROUP, msg_id, msg_id, 1
            ),
            event=LogEvent.REDIS_RECONNECTING,
        )
        return cast(int, pending[0]["times_delivered"]) if pending else 0
