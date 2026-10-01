"""Shared asyncio Redis client for the ingestion and processing entrypoints."""

from redis.asyncio import Redis

from core.config import get_redis_settings

settings = get_redis_settings()

redis_client: Redis = Redis(
    host=settings.host,
    port=settings.port,
    db=0,
    password=settings.password.get_secret_value(),
    decode_responses=True,
)


async def check_redis_con() -> None:
    await redis_client.ping()
