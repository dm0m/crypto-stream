import asyncio

import pytest
from redis.asyncio import Redis

from domain.enums import Exchange
from domain.trade import Trade
from processing.worker import TradeWorker
from storage.repositories import TradeRepository
from tests.helpers import (
    make_trade,
    make_trades,
    set_shutdown_event_after_delay,
    set_shutdown_event_when_drained,
)


@pytest.mark.asyncio(loop_scope="session")
async def test_duplicate_stream_entry_does_not_duplicate_row(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
) -> None:
    """XADDs the same trade payload twice and lets one real TradeWorker consume
    both entries, then asserts Postgres holds exactly one row for that trade_id.
    """
    event = asyncio.Event()
    trade: Trade = make_trade()
    worker: TradeWorker = TradeWorker(
        consumer="test-worker-1",
        recovery_consumer="test-recovery-consumer-1",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
    )
    set_shutdown_event_task = asyncio.create_task(
        set_shutdown_event_after_delay(0.3, event)
    )
    run_worker_task = asyncio.create_task(worker.run())
    await worker.ensure_group()
    await integration_redis_client.xadd(
        worker.STREAM, {"data": trade.model_dump_json()}
    )
    await integration_redis_client.xadd(
        worker.STREAM, {"data": trade.model_dump_json()}
    )
    await asyncio.gather(set_shutdown_event_task, run_worker_task)
    assert (
        await integration_trade_repository.get_trade_count(
            trade.trade_id, trade.exchange
        )
        == 1
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_recovered_pending_entry_does_not_duplicate_row(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
) -> None:
    """Reproduces the crash window between insert and XACK, then proves the
    recovery path cannot turn that replay into a duplicate row.
    """
    event = asyncio.Event()
    worker = TradeWorker(
        consumer="test-worker-1",
        recovery_consumer="test-recovery-consumer-1",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
    )
    trade: Trade = make_trade()
    await worker.ensure_group()
    await integration_redis_client.xadd(
        worker.STREAM, {"data": trade.model_dump_json()}
    )
    await integration_redis_client.xreadgroup(
        worker.GROUP, worker.CONSUMER, {worker.STREAM: ">"}
    )
    await integration_trade_repository.insert(trade)
    _, entries, _ = await integration_redis_client.xautoclaim(
        worker.STREAM, worker.GROUP, worker.RECOVERY_CONSUMER, 0
    )
    assert entries, (
        f"Expected at least one entry in {worker.STREAM} stream but got none"
    )
    for _, fields in entries:
        trade = Trade.model_validate_json(fields["data"])
        await integration_trade_repository.insert(trade)
    assert (
        await integration_trade_repository.get_trade_count(
            trade.trade_id, trade.exchange
        )
        == 1
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_two_workers_in_same_group_split_the_stream(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
) -> None:
    """Runs two TradeWorkers with distinct consumer names in the same group
    against one preloaded stream, and checks the group split the work between
    them without losing or duplicating anything.
    """

    event = asyncio.Event()
    worker_1: TradeWorker = TradeWorker(
        consumer="test-worker-1",
        recovery_consumer="test-recovery-consumer-1",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
    )
    worker_2: TradeWorker = TradeWorker(
        consumer="test-worker-2",
        recovery_consumer="test-recovery-consumer-2",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
    )
    await worker_1.ensure_group()

    # create and send trades to redis stream
    NO_OF_TRADES = 10000
    trades: list[Trade] = make_trades(NO_OF_TRADES)
    pipe = integration_redis_client.pipeline(transaction=True)
    for trade in trades:
        pipe.xadd(worker_1.STREAM, {"data": trade.model_dump_json()})
    await pipe.execute()

    # concurrently run both workers, sleep and shutdown
    def is_drained() -> bool:
        return worker_1.processed_count + worker_2.processed_count >= NO_OF_TRADES

    set_shutdown_event_task = asyncio.create_task(
        set_shutdown_event_when_drained(event, is_drained)
    )
    worker_1_run_task = asyncio.create_task(worker_1.run())
    worker_2_run_task = asyncio.create_task(worker_2.run())
    await asyncio.gather(set_shutdown_event_task, worker_1_run_task, worker_2_run_task)

    # assert both consumers have 0 pending
    consumer_info = await integration_redis_client.xinfo_consumers(
        "trades.raw", "trades"
    )
    consumer_info_by_name = {c["name"]: c for c in consumer_info}
    assert worker_1.CONSUMER in consumer_info_by_name, (
        f"{worker_1.CONSUMER} never registered with group {worker_1.GROUP}; registered consumers: {list(consumer_info_by_name)}"
    )
    assert worker_2.CONSUMER in consumer_info_by_name, (
        f"{worker_2.CONSUMER} never registered with group {worker_2.GROUP}; registered consumers: {list(consumer_info_by_name)}"
    )
    worker_1_pending_count = consumer_info_by_name[worker_1.CONSUMER]["pending"]
    worker_2_pending_count = consumer_info_by_name[worker_2.CONSUMER]["pending"]
    assert worker_1_pending_count == 0, (
        f"{worker_1.CONSUMER} left {worker_1_pending_count} entries unacked in the PEL"
    )
    assert worker_2_pending_count == 0, (
        f"{worker_2.CONSUMER} left {worker_2_pending_count} entries unacked in the PEL"
    )

    # assert both workers participated
    assert worker_1.processed_count > 0, (
        f"worker_1 consumed nothing; worker_2 consumed {worker_2.processed_count} of {NO_OF_TRADES}, stream was not split"
    )
    assert worker_2.processed_count > 0, (
        f"worker_2 consumed nothing; worker_1 consumed {worker_1.processed_count} of {NO_OF_TRADES}, stream was not split"
    )

    # asert both wrorkers processed_count add up to NO_OF_TRADES
    total = worker_1.processed_count + worker_2.processed_count
    assert total == NO_OF_TRADES, (
        f"consumed {worker_1.processed_count} + {worker_2.processed_count} = {total}, "
        f"expected {NO_OF_TRADES} (short = messages lost, over = delivered twice)"
    )

    # assert NO_OF_TRADES rows made it to db
    db_count = await integration_trade_repository.get_count(Exchange.BINANCE)
    assert db_count == NO_OF_TRADES, (
        f"db has {db_count} rows, expected {NO_OF_TRADES}; "
        f"workers reported {total} consumed"
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_same_trade_id_on_different_exchanges_are_distinct_rows(
    integration_trade_repository: TradeRepository,
) -> None:
    """The dedup key is (exchange, trade_id), not trade_id alone: the same id
    from two venues must land as two rows. Guards the composite unique index
    against being simplified to trade_id, which would silently drop every
    cross-exchange collision.
    """
    binance_trade = make_trade()
    kraken_trade = binance_trade.model_copy(update={"exchange": Exchange.KRAKEN})
    await integration_trade_repository.bulk_insert([binance_trade, kraken_trade])
    assert (
        await integration_trade_repository.get_trade_count(
            binance_trade.trade_id, Exchange.BINANCE
        )
        == 1
    )
    assert (
        await integration_trade_repository.get_trade_count(
            kraken_trade.trade_id, Exchange.KRAKEN
        )
        == 1
    )
