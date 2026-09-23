"""Data access for the ``trades`` table."""

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain.enums import Exchange
from domain.trade import Trade
from storage.models import TradeTable


class TradeRepository:
    """Data-access layer for the ``trades`` table."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory

    async def insert(self, trade: Trade) -> None:
        await self.bulk_insert([trade])

    async def bulk_insert(self, trades: list[Trade]) -> None:
        """Write a batch of trades in one transaction, skipping conflicts."""
        stmt = pg_insert(TradeTable).on_conflict_do_nothing(
            index_elements=["ts_event", "exchange", "trade_id"]
        )
        async with self._session_factory.begin() as session:
            await session.execute(stmt, [t.model_dump() for t in trades])

    async def get_trade_count(self, trade_id: str, exchange: Exchange) -> int:
        """Count rows for one trade identity, expected to be 0 or 1."""
        stmt = (
            select(func.count())
            .select_from(TradeTable)
            .where(TradeTable.trade_id == trade_id, TradeTable.exchange == exchange)
        )
        async with self._session_factory.begin() as session:
            return (await session.execute(stmt)).scalar_one()

    async def get_count(self, exchange: Exchange) -> int:
        """Count every stored trade for one exchange."""
        stmt = (
            select(func.count())
            .select_from(TradeTable)
            .where(TradeTable.exchange == exchange)
        )
        async with self._session_factory.begin() as session:
            return (await session.execute(stmt)).scalar_one()
