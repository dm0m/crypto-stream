"""Process-wide async SQLAlchemy engine and session factory."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from core.config import get_database_settings

# Pool of 10 persistent connections, bursting to 30 under load. pool_pre_ping
# issues a cheap check before handing out a pooled connection, so one that died
# during a Postgres restart is replaced instead of surfacing as an error.
engine: AsyncEngine = create_async_engine(
    get_database_settings().url, pool_size=10, max_overflow=20, pool_pre_ping=True
)
# expire_on_commit=False keeps ORM objects readable after commit; with the
# async driver an implicit refresh on attribute access would fail.
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def check_db_conn() -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
