import logging
from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from core.logging import configure_logging
from storage.models import Base
from storage.repositories import CandleRepository, TradeRepository


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    with PostgresContainer(
        "timescale/timescaledb:latest-pg17", driver="asyncpg"
    ) as container:
        yield container


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer, None, None]:
    with RedisContainer("redis:8.0.0") as container:
        yield container


@pytest.fixture(scope="session")
def migrated_database_url(postgres_container: PostgresContainer) -> str:
    """Bring the container to Alembic ``head`` and return its URL."""
    url = postgres_container.get_connection_url()
    config = Config("alembic.ini")
    config.attributes["sqlalchemy_url"] = url
    command.upgrade(config, "head")
    return url


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def integration_engine(
    migrated_database_url: str,
) -> AsyncGenerator[AsyncEngine, None]:
    """Real asyncpg engine against the migrated container database."""
    engine = create_async_engine(migrated_database_url)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def integration_session_factory(
    integration_engine: AsyncEngine,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(integration_engine, expire_on_commit=False)


@pytest.fixture(scope="session")
def integration_trade_repository(
    integration_session_factory: async_sessionmaker[AsyncSession],
) -> TradeRepository:
    return TradeRepository(integration_session_factory)


@pytest.fixture(scope="session")
def integration_candle_repository(
    integration_session_factory: async_sessionmaker[AsyncSession],
) -> CandleRepository:
    return CandleRepository(integration_session_factory)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def integration_redis_client(
    redis_container: RedisContainer,
) -> AsyncGenerator[Redis, None]:
    """Asyncio Redis client on the container, pinged once so a dead container
    fails setup instead of the first test. Closed at session end."""
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        db=0,
        decode_responses=True,
    )
    await client.ping()
    yield client
    await client.aclose(True)


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _clean_state(
    integration_engine: AsyncEngine,
    integration_redis_client: Redis,
) -> AsyncGenerator[None, None]:
    """Function-scoped: truncate Postgres + flush Redis before every integration
    test, so tests stay isolated without paying to restart containers each time."""
    async with integration_engine.begin() as conn:
        for table in ("trades", "candles"):
            await conn.execute(Base.metadata.tables[table].delete())
    await integration_redis_client.flushdb()
    yield


@pytest.fixture(autouse=True, scope="session")
def get_logger() -> None:
    configure_logging("logs/tests/app.log", logging.INFO)
