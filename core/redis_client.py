"""Shared asyncio Redis client for the ingestion and processing entrypoints."""

from redis.asyncio import Redis

from core.config import get_settings

settings = get_settings()
redis_client: Redis = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=0,
    password=settings.redis_password.get_secret_value(),
    decode_responses=True,
)


async def check_redis_con() -> None:
    await redis_client.ping()
