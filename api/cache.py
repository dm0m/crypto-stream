"""Response cache for the API, stored in Redis."""

import hashlib
from datetime import datetime, timedelta

import redis
import structlog
from pydantic import BaseModel
from redis.asyncio import Redis

from api.settings import ApiSettings
from core.log_events import LogEvent
from domain.enums import Interval


def ttl_for(
    *,
    ts_to: datetime,
    cursor: datetime | None,
    interval: Interval,
    now: datetime,
    settings: ApiSettings,
) -> int:
    """Choose how long a response may be cached, from how current its data is."""
    newest = min(ts_to, cursor) if cursor is not None else ts_to
    settled_before = interval.floor(
        now - timedelta(seconds=settings.cache_settle_seconds)
    )
    if newest > settled_before:
        return settings.cache_ttl_live_seconds
    return settings.cache_ttl_closed_seconds


class ResponseCache:
    """Stores serialized API responses in Redis under request-derived keys."""

    def __init__(self, redis_client: Redis, namespace: str = "cs:v1") -> None:
        """Wrap ``redis_client``; no connection is made here."""
        self._redis_client = redis_client
        self._namespace = namespace
        self._available = True
        self._logger: structlog.BoundLogger = structlog.get_logger().bind(
            service="api", namespace=namespace
        )

    def key(self, endpoint: str, *parts: str, query: BaseModel) -> str:
        """Build the key that identifies one request's response."""
        digest = hashlib.sha256(query.model_dump_json().encode()).hexdigest()[:16]
        return ":".join((self._namespace, endpoint, *parts, digest))

    async def get(self, key: str) -> str | None:
        """Return the stored response for ``key``, or ``None`` on a miss."""
        try:
            value = await self._redis_client.get(key)
        except redis.RedisError as exc:
            self._mark_unavailable("get", exc)
            return None
        self._mark_available()
        if isinstance(value, str):
            self._logger.debug(LogEvent.CACHE_HIT, key=key)
            return value
        self._logger.debug(LogEvent.CACHE_MISS, key=key)
        return None

    async def set(self, key: str, value: str, ttl: int) -> None:
        """Store ``value`` under ``key`` for ``ttl`` seconds."""
        try:
            await self._redis_client.set(key, value, ex=ttl)
        except redis.RedisError as exc:
            self._mark_unavailable("set", exc)
            return
        self._mark_available()

    def _mark_unavailable(self, operation: str, exc: redis.RedisError) -> None:
        """Record a failure, logging it only if Redis had been answering."""
        if self._available:
            self._logger.warning(
                LogEvent.CACHE_UNAVAILABLE,
                operation=operation,
                error=type(exc).__name__,
                detail=str(exc),
            )
        self._available = False

    def _mark_available(self) -> None:
        """Record a success, logging it only if Redis had been failing."""
        if not self._available:
            self._logger.info(LogEvent.CACHE_RECOVERED)
        self._available = True
