"""Keyset pagination over the continuous aggregates, against real TimescaleDB."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from domain.enums import Exchange, Interval, Side
from domain.trade import Trade
from storage.repositories import CandleRepository, TradeRepository

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
BUCKETS = 12
WINDOW_END = T0 + timedelta(minutes=BUCKETS)
# A window no test ever seeds, used to assert the empty-result behaviour.
EMPTY_WINDOW_START = T0 - timedelta(days=30)
EMPTY_WINDOW_END = EMPTY_WINDOW_START + timedelta(hours=1)


def trade_at(ts: datetime, price: str, trade_id: str) -> Trade:
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


async def seed(
    trade_repository: TradeRepository, engine: AsyncEngine, buckets: int = BUCKETS
) -> None:
    """Insert one trade per minute and materialize them into ``candles_1m``."""
    await trade_repository.bulk_insert(
        [
            trade_at(T0 + timedelta(minutes=i), str(100 + i), f"t{i}")
            for i in range(buckets)
        ]
    )
    async with engine.connect() as conn:
        autocommit = await conn.execution_options(isolation_level="AUTOCOMMIT")
        await autocommit.execute(
            text("CALL refresh_continuous_aggregate('candles_1m', NULL, NULL)")
        )


async def walk(repository: CandleRepository, limit: int) -> list[datetime]:
    """Page through the whole window and return every bucket start seen."""
    seen: list[datetime] = []
    cursor: datetime | None = None
    for _ in range(BUCKETS + 2):
        page = await repository.get_candles(
            exchange=Exchange.BINANCE,
            symbol="BTC-USDT",
            interval=Interval.M1,
            ts_from=T0,
            ts_to=WINDOW_END,
            limit=limit,
            cursor=cursor,
        )
        seen.extend(candle.ts_open for candle in page.candles)
        if page.next_cursor is None:
            return seen
        cursor = page.next_cursor
    pytest.fail("cursor never reached the end of the result set")


@pytest.mark.asyncio(loop_scope="session")
@pytest.mark.parametrize("limit", [1, 5, BUCKETS - 1, BUCKETS, BUCKETS + 1])
async def test_paging_yields_every_bucket_exactly_once(
    limit: int,
    integration_trade_repository: TradeRepository,
    integration_candle_repository: CandleRepository,
    integration_engine: AsyncEngine,
) -> None:
    """No row is skipped or repeated, at any page size."""
    await seed(integration_trade_repository, integration_engine)
    seen = await walk(integration_candle_repository, limit)
    assert len(seen) == BUCKETS
    assert len(set(seen)) == BUCKETS
    assert seen == sorted(seen, reverse=True)


@pytest.mark.asyncio(loop_scope="session")
async def test_paged_result_matches_a_single_large_page(
    integration_trade_repository: TradeRepository,
    integration_candle_repository: CandleRepository,
    integration_engine: AsyncEngine,
) -> None:
    """Paging changes how rows arrive, never which rows or in what order."""
    await seed(integration_trade_repository, integration_engine)
    whole = await walk(integration_candle_repository, BUCKETS * 2)
    in_pages = await walk(integration_candle_repository, 5)
    assert in_pages == whole


@pytest.mark.asyncio(loop_scope="session")
async def test_last_page_reports_no_further_cursor(
    integration_trade_repository: TradeRepository,
    integration_candle_repository: CandleRepository,
    integration_engine: AsyncEngine,
) -> None:
    """A cursor on the final page would send clients round an extra time."""
    await seed(integration_trade_repository, integration_engine, buckets=3)
    page = await integration_candle_repository.get_candles(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        interval=Interval.M1,
        ts_from=T0,
        ts_to=WINDOW_END,
        limit=3,
    )
    assert len(page.candles) == 3
    assert page.next_cursor is None


@pytest.mark.asyncio(loop_scope="session")
async def test_empty_window_returns_no_candles_and_no_cursor(
    integration_candle_repository: CandleRepository,
) -> None:
    """An empty result is an empty page, not an error and not a cursor."""
    page = await integration_candle_repository.get_candles(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        interval=Interval.M1,
        ts_from=EMPTY_WINDOW_START,
        ts_to=EMPTY_WINDOW_END,
        limit=10,
    )
    assert page.candles == []
    assert page.next_cursor is None
