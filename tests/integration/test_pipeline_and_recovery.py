"""End-to-end producer to consumer contract, and the poison-entry DLQ route,
against real Redis and Postgres. The other integration tests XADD hand-built
entries, so neither of these paths was exercised before."""

import asyncio
from typing import cast

import pytest
from redis.asyncio import Redis

from domain.enums import Exchange
from ingestion.service import IngestionService
from processing.worker import StreamEntry, TradeWorker
from storage.repositories import TradeRepository
from tests.helpers import make_trades, set_shutdown_event_when_drained


@pytest.mark.asyncio(loop_scope="session")
async def test_ingestion_flush_is_consumed_by_worker(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
) -> None:
    """``IngestionService.flush_batch`` writes entries a real ``TradeWorker``
    reads, parses and persists, and the worker acks all of them.
    """
    event = asyncio.Event()
    service = IngestionService(integration_redis_client)
    worker = TradeWorker(
        consumer="test-worker-1",
        recovery_consumer="test-recovery-consumer-1",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
    )
    trades = make_trades(25)
    await worker.ensure_group()
    await service.flush_batch(trades)

    def is_drained() -> bool:
        return worker.processed_count >= len(trades)

    set_shutdown_event_task = asyncio.create_task(
        set_shutdown_event_when_drained(event, is_drained)
    )
    await asyncio.gather(set_shutdown_event_task, worker.run())

    assert worker.processed_count == len(trades)
    assert await integration_trade_repository.get_count(Exchange.BINANCE) == len(trades)
    consumer_info = {
        c["name"]: c
        for c in await integration_redis_client.xinfo_consumers(
            worker.STREAM, worker.GROUP
        )
    }
    assert consumer_info[worker.CONSUMER]["pending"] == 0, (
        "worker read entries it never acked"
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_poison_entry_reaches_dlq_after_max_retries(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
) -> None:
    """An entry that can never parse is dead-lettered on its MAX_RETRIES-th
    delivery and leaves the PEL, so it stops blocking recovery forever.
    """
    worker = TradeWorker(
        consumer="test-worker-1",
        recovery_consumer="test-recovery-consumer-1",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=asyncio.Event(),
    )
    worker.MIN_IDLE_TIME = 0
    await worker.ensure_group()
    poison_id = cast(
        str,
        await integration_redis_client.xadd(worker.STREAM, {"data": "not a trade"}),
    )
    await integration_redis_client.xreadgroup(
        worker.GROUP, worker.CONSUMER, {worker.STREAM: ">"}
    )
    assert await worker.get_delivery_count(poison_id) == 1

    await worker.recover_failed_msgs()
    assert await worker.get_delivery_count(poison_id) == 2, (
        "first sweep should reclaim, fail to parse, and leave the entry pending"
    )
    assert await integration_redis_client.xlen(worker.DLQ_STREAM) == 0

    await worker.recover_failed_msgs()
    assert await worker.get_delivery_count(poison_id) == 0, (
        "entry still in the PEL after DLQ routing"
    )
    dlq = cast(
        list[StreamEntry], await integration_redis_client.xrange(worker.DLQ_STREAM)
    )
    assert len(dlq) == 1
    _, fields = dlq[0]
    assert fields["message_id"] == poison_id
    assert fields["delivery_count"] == str(TradeWorker.MAX_RETRIES)
    assert fields["error"] == "max_retries_exceeded"
    assert fields["data"] == "not a trade"
    assert await integration_trade_repository.get_count(Exchange.BINANCE) == 0
