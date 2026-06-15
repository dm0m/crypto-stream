from domain.trade import Trade
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from storage.models import TradeTable


class TradeRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._session_factory = session_factory
        
    async def bulk_insert(self, trades: list[Trade]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                stmt = pg_insert(TradeTable).on_conflict_do_nothing(index_elements=["exchange", "trade_id"])
                await session.execute(stmt, [t.model_dump() for t in trades])

    