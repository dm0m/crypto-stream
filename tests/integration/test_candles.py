"""Candle aggregation end to end: stream entries in, ``candles`` rows out,
against real Redis and TimescaleDB."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from domain.candle import Candle
from domain.enums import Exchange, Interval, Side
from domain.trade import Trade
from ingestion.service import IngestionService
from processing.aggregator import CandleAggregator
from processing.worker import TradeWorker
from storage.models import CandleTable
from storage.repositories import CandleRepository, TradeRepository
from tests.helpers import run_workers_until_drained

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


def trade_at(ts: datetime, price: str, qty: str, trade_id: str) -> Trade:
    return Trade(
        trade_id=trade_id,
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        price=Decimal(price),
        quantity=Decimal(qty),
        side=Side.BUY,
        ts_event=ts,
        ts_ingest=ts,
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_worker_writes_closed_candle_matching_hand_computed_ohlcv(
    integration_redis_client: Redis,
    integration_trade_repository: TradeRepository,
    integration_candle_repository: CandleRepository,
    integration_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Three trades in minute T0 and one in T0+1: exactly one ``candles`` row
    for T0, with the values computed by hand. The T0+1 bucket stays open
    (its ``ts_open`` is far in the past, but the flush timer compares against
    wall-clock ``now``, so it would close within a second; the worker is
    stopped as soon as the batch is processed to keep the assertion exact).
    """
    event = asyncio.Event()
    service = IngestionService(integration_redis_client)
    worker = TradeWorker(
        consumer="test-worker-candles",
        recovery_consumer="test-recovery-candles",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
        aggregator=CandleAggregator(Interval.M1, timedelta(hours=24)),
        candle_repository=integration_candle_repository,
    )
    trades = [
        trade_at(T0, "100", "1", "1"),
        trade_at(T0 + timedelta(seconds=20), "130", "2", "2"),
        trade_at(T0 + timedelta(seconds=40), "80", "3", "3"),
        trade_at(T0 + timedelta(minutes=1), "999", "1", "4"),
    ]
    await worker.ensure_group()
    await service.flush_batch(trades)

    await run_workers_until_drained(event, len(trades), worker)

    assert await integration_trade_repository.get_count(Exchange.BINANCE) == 4
    assert (
        await integration_candle_repository.get_count(Exchange.BINANCE, Interval.M1)
        == 1
    )
    async with integration_session_factory() as session:
        row = (await session.execute(select(CandleTable))).scalar_one()
    assert row.ts_open == T0
    assert row.interval is Interval.M1
    assert (row.open, row.high, row.low, row.close) == (
        Decimal("100"),
        Decimal("130"),
        Decimal("80"),
        Decimal("80"),
    )
    assert row.volume == Decimal("6")
    assert row.trade_count == 3


@pytest.mark.asyncio(loop_scope="session")
async def test_upsert_keeps_first_candle_per_bucket(
    integration_candle_repository: CandleRepository,
) -> None:
    """A second candle for the same key is ignored, never merged or replaced."""

    def candle(close: str, count: int) -> Candle:
        return Candle(
            exchange=Exchange.BINANCE,
            symbol="BTC-USDT",
            interval=Interval.M1,
            ts_open=T0,
            open=Decimal("100"),
            high=Decimal("100"),
            low=Decimal(close),
            close=Decimal(close),
            volume=Decimal(count),
            trade_count=count,
        )

    await integration_candle_repository.upsert([candle("90", 5)])
    await integration_candle_repository.upsert([candle("50", 1)])
    assert (
        await integration_candle_repository.get_count(Exchange.BINANCE, Interval.M1)
        == 1
    )


@pytest.mark.asyncio(loop_scope="session")
async def test_migrations_create_hypertables_and_continuous_aggregates(
    integration_engine: AsyncEngine,
) -> None:
    """The replayed migrations leave the Timescale objects step 4 relies on."""
    async with integration_engine.connect() as conn:
        hypertables = set(
            (
                await conn.execute(
                    text(
                        "SELECT hypertable_name "
                        "FROM timescaledb_information.hypertables"
                    )
                )
            ).scalars()
        )
        aggregates = set(
            (
                await conn.execute(
                    text(
                        "SELECT view_name "
                        "FROM timescaledb_information.continuous_aggregates"
                    )
                )
            ).scalars()
        )
        trade_jobs = set(
            (
                await conn.execute(
                    text(
                        "SELECT proc_name FROM timescaledb_information.jobs "
                        "WHERE hypertable_name = 'trades'"
                    )
                )
            ).scalars()
        )
    assert {"trades", "candles"} <= hypertables
    assert aggregates == {"candles_1m", "candles_1h"}
    assert {"policy_compression", "policy_retention"} <= trade_jobs


@pytest.mark.asyncio(loop_scope="session")
async def test_continuous_aggregate_agrees_with_worker_candles(
    integration_redis_client: Redis,
    integration_engine: AsyncEngine,
    integration_trade_repository: TradeRepository,
    integration_candle_repository: CandleRepository,
    integration_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The in-memory path and the recomputed-from-trades path produce the
    same 1m candle. This is the check that makes keeping both defensible.
    """
    event = asyncio.Event()
    service = IngestionService(integration_redis_client)
    worker = TradeWorker(
        consumer="test-worker-cagg",
        recovery_consumer="test-recovery-cagg",
        redis_client=integration_redis_client,
        trade_repository=integration_trade_repository,
        shutdown_event=event,
        aggregator=CandleAggregator(Interval.M1, timedelta(hours=24)),
        candle_repository=integration_candle_repository,
    )
    trades = [
        trade_at(T0 + timedelta(seconds=i * 7), str(100 + (i * 37) % 50), "0.5", str(i))
        for i in range(8)
    ] + [trade_at(T0 + timedelta(minutes=1), "1", "1", "roll")]
    await worker.ensure_group()
    await service.flush_batch(trades)

    await run_workers_until_drained(event, len(trades), worker)

    async with integration_engine.connect() as conn:
        autocommit = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(
            text("CALL refresh_continuous_aggregate('candles_1m', NULL, NULL)")
        )
        cagg = (
            await autocommit.execute(
                text(
                    "SELECT open, high, low, close, volume, trade_count "
                    "FROM candles_1m WHERE ts_open = :bucket"
                ),
                {"bucket": T0},
            )
        ).one()

    async with integration_session_factory() as session:
        row = (
            await session.execute(select(CandleTable).where(CandleTable.ts_open == T0))
        ).scalar_one()

    assert (row.open, row.high, row.low, row.close) == tuple(cagg[:4])
    assert row.volume == cagg.volume
    assert row.trade_count == cagg.trade_count == 8
