"""Data access for candles: the worker-written table and the aggregates."""

from datetime import datetime
from decimal import Decimal
from typing import NamedTuple

import structlog
from sqlalchemy import ColumnElement, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain.candle import Candle
from domain.enums import Exchange, Interval
from storage.models import CandleTable
from storage.views import view_for


class CandlePage(NamedTuple):
    """One page of candles and the key to resume from."""

    candles: list[Candle]
    next_cursor: datetime | None


class SymbolStats(NamedTuple):
    as_of: datetime
    exchange: str
    last_price: Decimal
    volume: Decimal
    vwap: Decimal


class CandleRepository:
    """Data-access layer for the worker-produced ``candles`` table."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        """Store the session factory; no connection is made here."""
        self._session_factory = session_factory
        self._logger: structlog.BoundLogger = structlog.get_logger().bind(
            service="candle.repository"
        )

    async def upsert(self, candles: list[Candle]) -> None:
        """Write a batch of closed candles, keeping the first one per bucket."""
        stmt = pg_insert(CandleTable).on_conflict_do_nothing(
            index_elements=["ts_open", "exchange", "symbol", "interval"]
        )
        async with self._session_factory.begin() as session:
            await session.execute(stmt, [c.model_dump() for c in candles])

    async def get_count(self, exchange: Exchange, interval: Interval) -> int:
        """Count stored candles for one exchange and interval."""
        stmt = (
            select(func.count())
            .select_from(CandleTable)
            .where(CandleTable.exchange == exchange, CandleTable.interval == interval)
        )
        async with self._session_factory.begin() as session:
            return (await session.execute(stmt)).scalar_one()

    async def get_candles(
        self,
        *,
        exchange: Exchange,
        symbol: str,
        interval: Interval,
        ts_from: datetime,
        ts_to: datetime,
        limit: int,
        cursor: datetime | None = None,
    ) -> CandlePage:
        """Read one page of candles from the aggregate for ``interval``."""
        view = view_for(interval)
        conditions: list[ColumnElement[bool]] = [
            view.c.exchange == exchange,
            view.c.symbol == symbol,
            view.c.ts_open >= ts_from,
            view.c.ts_open < ts_to,
        ]
        if cursor is not None:
            conditions.append(view.c.ts_open < cursor)
        stmt = (
            select(
                view.c.ts_open,
                view.c.exchange,
                view.c.symbol,
                view.c.open,
                view.c.high,
                view.c.low,
                view.c.close,
                view.c.volume,
                view.c.trade_count,
            )
            .where(*conditions)
            .order_by(view.c.ts_open.desc())
            .limit(limit + 1)
        )
        async with self._session_factory.begin() as session:
            rows = (await session.execute(stmt)).mappings().all()
        has_more = len(rows) > limit
        candles = [Candle(interval=interval, **row) for row in rows[:limit]]
        next_cursor = candles[-1].ts_open if has_more and candles else None
        return CandlePage(candles, next_cursor)

    async def get_stats(
        self, *, symbol: str, exchange: Exchange, ts_from: datetime, ts_to: datetime
    ) -> SymbolStats | None:
        view = view_for(Interval.M1)
        stmt = select(
            func.max(view.c.ts_open).label("as_of"),
            func.last(view.c.close, view.c.ts_open).label("last_price"),
            func.sum(view.c.volume).label("volume"),
            (
                func.sum(view.c.quote_volume) / func.nullif(func.sum(view.c.volume), 0)
            ).label("vwap"),
        ).where(
            view.c.exchange == exchange,
            view.c.symbol == symbol,
            view.c.ts_open >= ts_from,
            view.c.ts_open < ts_to,
        )
        async with self._session_factory.begin() as session:
            row = (await session.execute(stmt)).one()
        if row.as_of is None:
            return None
        return SymbolStats(
            as_of=row.as_of,
            exchange=exchange,
            last_price=row.last_price,
            volume=row.volume,
            vwap=row.vwap,
        )
