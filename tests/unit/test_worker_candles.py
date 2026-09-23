"""Unit tests for the candle-aggregation path of ``TradeWorker``."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio import Redis

from domain.enums import Exchange, Interval, Side
from domain.trade import Trade
from processing.aggregator import CandleAggregator
from processing.worker import TradeWorker
from storage.repositories import TradeRepository

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def trade_at(ts: datetime, price: str = "100", trade_id: str = "1") -> Trade:
    return Trade(
        trade_id=trade_id,
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=Side.BUY,
        ts_event=ts,
        ts_ingest=ts,
    )


def test_aggregator_and_candle_repository_must_come_together(
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    with pytest.raises(ValueError, match="together"):
        TradeWorker(
            consumer="w",
            recovery_consumer="r",
            redis_client=cast(Redis, fake_redis),
            trade_repository=cast(TradeRepository, fake_trade_repository),
            shutdown_event=shutdown_event,
            aggregator=CandleAggregator(Interval.M1, timedelta(seconds=2)),
        )


@pytest.mark.asyncio
async def test_flush_without_aggregator_never_touches_candles(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
) -> None:
    await worker._flush([trade_at(T0)], ["1-0"])
    fake_trade_repository.bulk_insert.assert_awaited_once()
    fake_redis.xack.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_with_open_bucket_only_writes_no_candles(
    aggregating_worker: TradeWorker,
    fake_candle_repository: AsyncMock,
    fake_redis: AsyncMock,
) -> None:
    """Trades within one minute open a bucket but close nothing yet."""
    trades = [
        trade_at(T0, "100", "1"),
        trade_at(T0 + timedelta(seconds=30), "110", "2"),
    ]
    await aggregating_worker._flush(trades, ["1-0", "2-0"])
    fake_candle_repository.upsert.assert_not_awaited()
    fake_redis.xack.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_upserts_closed_candle_before_ack(
    aggregating_worker: TradeWorker,
    fake_trade_repository: AsyncMock,
    fake_candle_repository: AsyncMock,
    fake_redis: AsyncMock,
) -> None:
    order: list[str] = []
    fake_trade_repository.bulk_insert.side_effect = lambda *_: order.append("insert")
    fake_candle_repository.upsert.side_effect = lambda *_: order.append("candles")
    fake_redis.xack.side_effect = lambda *_: order.append("ack")

    trades = [trade_at(T0, "100", "1"), trade_at(T0 + timedelta(minutes=1), "200", "2")]
    await aggregating_worker._flush(trades, ["1-0", "2-0"])

    assert order == ["insert", "candles", "ack"]
    candles = fake_candle_repository.upsert.await_args.args[0]
    assert len(candles) == 1
    assert candles[0].ts_open == T0
    assert candles[0].close == Decimal("100")


@pytest.mark.asyncio
async def test_flush_candles_timer_closes_expired_bucket(
    aggregating_worker: TradeWorker,
    fake_candle_repository: AsyncMock,
) -> None:
    """The timer writes a candle for a series with no further trades."""
    await aggregating_worker._flush([trade_at(T0)], ["1-0"])
    fake_candle_repository.upsert.assert_not_awaited()

    well_after = T0 + timedelta(minutes=5)
    with (
        patch("processing.worker.datetime") as fake_datetime,
        patch("processing.worker.asyncio.sleep", new=AsyncMock()) as fake_sleep,
    ):
        fake_datetime.now.return_value = well_after
        # Let the loop run one iteration, then cancel it via the second sleep.
        fake_sleep.side_effect = [None, asyncio.CancelledError()]
        with pytest.raises(asyncio.CancelledError):
            await aggregating_worker.flush_candles(delay=0)

    fake_candle_repository.upsert.assert_awaited_once()
    candles = fake_candle_repository.upsert.await_args.args[0]
    assert [c.ts_open for c in candles] == [T0]


@pytest.mark.asyncio
async def test_flush_candles_returns_immediately_without_aggregator(
    worker: TradeWorker,
) -> None:
    await asyncio.wait_for(worker.flush_candles(delay=0), timeout=0.5)


@pytest.mark.asyncio
async def test_run_starts_and_cancels_flush_task_when_aggregating(
    aggregating_worker: TradeWorker,
) -> None:
    """Same shape as the recovery/DLQ cancellation test, plus the flush timer."""
    cancelled: list[str] = []

    async def _hang(name: str) -> None:
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def consume() -> None:
        await asyncio.sleep(0)  # let the background tasks start running

    w = aggregating_worker
    w.ensure_group = AsyncMock(return_value=None)  # type: ignore[method-assign]
    w.recover_failed_msgs = lambda: _hang("recover")  # type: ignore[method-assign]
    w.log_dlq_count = lambda delay=30: _hang("log_dlq")  # type: ignore[method-assign]
    w.flush_candles = lambda delay=1.0: _hang("flush")  # type: ignore[method-assign]
    w.consume = consume  # type: ignore[method-assign]
    await w.run()
    assert set(cancelled) == {"recover", "log_dlq", "flush"}


@pytest.mark.asyncio
async def test_run_does_not_start_flush_task_without_aggregator(
    worker: TradeWorker,
) -> None:
    called = False

    async def flush(delay: float = 1.0) -> None:
        nonlocal called
        called = True

    async def consume() -> None:
        await asyncio.sleep(0)

    worker.ensure_group = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.recover_failed_msgs = AsyncMock()  # type: ignore[method-assign]
    worker.log_dlq_count = AsyncMock()  # type: ignore[method-assign]
    worker.flush_candles = flush  # type: ignore[method-assign]
    worker.consume = consume  # type: ignore[method-assign]
    await worker.run()
    assert not called
