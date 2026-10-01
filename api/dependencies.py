from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from api.cache import ResponseCache
from api.settings import ApiSettings, get_api_settings
from core.redis_client import redis_client
from storage.engine import session_factory
from storage.repositories.candles import CandleRepository


def get_redis_client() -> Redis:
    return redis_client


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return session_factory


SessionFactoryDep = Annotated[
    async_sessionmaker[AsyncSession], Depends(get_session_factory)
]


def get_candle_repository(session_factory: SessionFactoryDep) -> CandleRepository:
    return CandleRepository(session_factory)


RedisDep = Annotated[Redis, Depends(get_redis_client)]


@lru_cache
def get_response_cache(redis: RedisDep) -> ResponseCache:
    return ResponseCache(redis)


CandleRepoDep = Annotated[CandleRepository, Depends(get_candle_repository)]
ResponseCacheDep = Annotated[ResponseCache, Depends(get_response_cache)]
SettingsDep = Annotated[ApiSettings, Depends(get_api_settings)]
