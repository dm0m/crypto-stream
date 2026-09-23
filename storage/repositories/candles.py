"""Data access for the worker-produced ``candles`` table."""

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain.candle import Candle
from domain.enums import Exchange, Interval
from storage.models import CandleTable


class CandleRepository:
    """Data-access layer for the worker-produced ``candles`` table."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

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
