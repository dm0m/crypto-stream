import asyncio
import json
from functools import partial
from typing import cast

import structlog
import websockets
from redis.asyncio import Redis

from core.backoff import backoff, race_wait_task, retry_call
from core.log_events import LogEvent
from domain.enums import Exchange
from domain.trade import Trade
from ingestion.normalizers.binance import BinanceNormalizer
from ingestion.schemas.binance import BinanceTradeRaw

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


class IngestionService:
    """Orchestrates the Binance ingestion pipeline."""

    def __init__(
        self,
        redis_client: Redis,
        shutdown_event: asyncio.Event | None = None,
        *,
        symbols: list[str] | None = None,
        batch_size: int = 1000,
        stream_maxlen: int = 500_000,
    ) -> None:
        """Wire up the service; no connection is opened until ``run``."""
        self._redis_client: Redis = redis_client
        self.symbols = symbols or ["btcusdt"]
        self.batch_size = batch_size
        self.stream_maxlen = stream_maxlen
        self.queue: asyncio.Queue[Trade] = asyncio.Queue(maxsize=batch_size)
        self.received = 0
        self.logger: structlog.BoundLogger = structlog.get_logger().bind(
            service="ingestion", exchange=Exchange.BINANCE
        )
        self._shutdown_event = shutdown_event

    async def run(self) -> None:
        """Run the pipeline until shutdown is requested."""
        self.logger.info(LogEvent.INGESTION_STARTED)
        write_to_redis_task = asyncio.create_task(self.write())
        report_metrics_task = asyncio.create_task(self._report_metrics())

        await backoff(
            self.connect_and_consume,
            event=LogEvent.WS_RECONNECTING,
            shutdown_event=self._shutdown_event,
        )

        if self._shutdown_event:
            self.logger.info(LogEvent.SHUTDOWN_INITIATED)
            report_metrics_task.cancel()
            await asyncio.gather(write_to_redis_task, return_exceptions=True)
            await asyncio.gather(report_metrics_task, return_exceptions=True)
            self.logger.info(LogEvent.SHUTDOWN_COMPLETE)

    async def connect_and_consume(self) -> None:
        """Open one WebSocket session, subscribe, and read it until it ends.

        Raises:
            websockets.ConnectionClosedError: The server dropped the
                connection with a non-clean close code.
            OSError: The connection could not be established or the socket
                failed at the transport level.
            asyncio.TimeoutError: The opening handshake timed out.
            websockets.InvalidHandshake: The server rejected the upgrade;
                not retried by ``backoff``, so this ends the service.
        """
        async with websockets.connect(BINANCE_WS_URL) as ws:
            subscribe_msg = json.dumps(
                {
                    "method": "SUBSCRIBE",
                    "params": [f"{s}@trade" for s in self.symbols],
                    "id": 1,
                }
            )
            await ws.send(subscribe_msg)
            if self._shutdown_event is None:
                await self.receive(ws)
            else:
                try:
                    receive_task = asyncio.create_task(self.receive(ws))
                    await race_wait_task(receive_task, self._shutdown_event)
                except asyncio.CancelledError:
                    pass

    async def receive(self, ws: websockets.ClientConnection) -> None:
        """Read messages from ``ws`` until it closes, queueing normalized trades."""
        self.logger.info(LogEvent.WS_CONNECTED, symbols=self.symbols)
        async for message in ws:
            try:
                raw_trade: BinanceTradeRaw = cast(BinanceTradeRaw, json.loads(message))
            except json.JSONDecodeError as e:
                self.logger.warning(LogEvent.TRADE_DROPPED, error=str(e), raw=message)
                continue
            trade = BinanceNormalizer.normalize(raw_trade)
            if trade is not None:
                await self.queue.put(trade)
                self.received += 1

    async def write(self) -> None:
        while True:
            trade = await self.get_next_trade()
            if trade is None:
                return
            batch: list[Trade] = [trade]
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await retry_call(
                partial(self.flush_batch, batch),
                event=LogEvent.REDIS_RECONNECTING,
                shutdown_event=self._shutdown_event,
            )

    async def get_next_trade(self) -> Trade | None:
        """Return the next trade, or None once shutdown fires with an empty queue."""
        if self._shutdown_event is None:
            return await self.queue.get()
        try:
            return await race_wait_task(
                asyncio.create_task(self.queue.get()), self._shutdown_event
            )
        except asyncio.CancelledError:
            return None

    async def flush_batch(self, batch: list[Trade]) -> None:
        """XADD every trade in ``batch`` to the ``trades.raw`` stream in one round trip.

        Raises:
            redis.exceptions.RedisError: The pipeline could not be executed.
                Connection-level subclasses are what ``retry_call`` retries
                when ``write`` wraps this call.
        """
        pipe = self._redis_client.pipeline(transaction=False)
        for trade in batch:
            pipe.xadd(
                "trades.raw",
                {"data": trade.model_dump_json()},
                maxlen=self.stream_maxlen,
                approximate=True,
            )
        await pipe.execute()

    async def _report_metrics(self, interval: int = 5) -> None:
        """Log a ``METRICS_SNAPSHOT`` every ``interval`` seconds until cancelled."""
        dropped_prev = BinanceNormalizer.malformed_trade_count
        received_prev = 0
        while True:
            await asyncio.sleep(interval)
            dropped_now = BinanceNormalizer.malformed_trade_count
            dropped_delta = dropped_now - dropped_prev
            received_now = self.received
            received_delta = received_now - received_prev
            drop_ratio = dropped_delta / received_delta if received_delta > 0 else 0.0
            queue_pct = self.queue.qsize() / self.queue.maxsize * 100
            self.logger.info(
                LogEvent.METRICS_SNAPSHOT,
                dropped_total=dropped_now,
                dropped_delta=dropped_delta,
                dropped_per_sec=dropped_delta / interval,
                received_per_sec=received_delta / interval,
                drop_ratio=drop_ratio,
                queue_pct=queue_pct,
            )
            dropped_prev = dropped_now
            received_prev = received_now
