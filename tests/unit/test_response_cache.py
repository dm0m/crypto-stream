"""Unit tests for ``ResponseCache`` reads, writes and failure logging."""

from collections.abc import MutableMapping
from typing import Any, cast

import pytest
import redis
from redis.asyncio import Redis
from structlog.testing import capture_logs

from api.cache import ResponseCache
from core.log_events import LogEvent


class FakeRedis:
    """In-memory stand-in for the two Redis commands the cache uses."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.down = False

    async def get(self, key: str) -> str | None:
        if self.down:
            raise redis.ConnectionError("Connection refused")
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int) -> None:
        if self.down:
            raise redis.ConnectionError("Connection refused")
        self.store[key] = value
        self.ttls[key] = ex


@pytest.fixture
def fake() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def cache(fake: FakeRedis) -> ResponseCache:
    return ResponseCache(cast(Redis, fake))


def events(
    logs: list[MutableMapping[str, Any]], event: LogEvent
) -> list[MutableMapping[str, Any]]:
    return [entry for entry in logs if entry["event"] == event]


@pytest.mark.asyncio
async def test_missing_key_is_a_miss(cache: ResponseCache) -> None:
    """An absent entry reads as ``None`` and is logged as a miss."""
    with capture_logs() as logs:
        assert await cache.get("k") is None
    assert len(events(logs, LogEvent.CACHE_MISS)) == 1


@pytest.mark.asyncio
async def test_stored_value_is_a_hit(cache: ResponseCache, fake: FakeRedis) -> None:
    """A stored response comes back as written, with the expiry passed through."""
    await cache.set("k", '{"a":1}', ttl=7)
    with capture_logs() as logs:
        assert await cache.get("k") == '{"a":1}'
    assert fake.ttls["k"] == 7
    assert len(events(logs, LogEvent.CACHE_HIT)) == 1


@pytest.mark.asyncio
async def test_redis_down_reads_as_a_miss(
    cache: ResponseCache, fake: FakeRedis
) -> None:
    """An unreachable cache must not raise into the request handler."""
    fake.down = True
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_redis_down_write_does_not_raise(
    cache: ResponseCache, fake: FakeRedis
) -> None:
    """Failing to store a computed response only means recomputing it later."""
    fake.down = True
    await cache.set("k", "v", ttl=5)


@pytest.mark.asyncio
async def test_outage_is_logged_once_not_per_request(
    cache: ResponseCache, fake: FakeRedis
) -> None:
    """Ten failed requests during one outage produce one warning."""
    fake.down = True
    with capture_logs() as logs:
        for _ in range(5):
            await cache.get("k")
            await cache.set("k", "v", ttl=5)
    warnings = events(logs, LogEvent.CACHE_UNAVAILABLE)
    assert len(warnings) == 1
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["error"] == "ConnectionError"


@pytest.mark.asyncio
async def test_recovery_is_logged_once(cache: ResponseCache, fake: FakeRedis) -> None:
    """The end of an outage is reported once, on the first successful call."""
    fake.down = True
    await cache.get("k")
    fake.down = False
    with capture_logs() as logs:
        await cache.get("k")
        await cache.get("k")
    assert len(events(logs, LogEvent.CACHE_RECOVERED)) == 1


@pytest.mark.asyncio
async def test_a_second_outage_is_logged_again(
    cache: ResponseCache, fake: FakeRedis
) -> None:
    """Once recovered, a new failure starts a new outage and is reported."""
    with capture_logs() as logs:
        fake.down = True
        await cache.get("k")
        fake.down = False
        await cache.get("k")
        fake.down = True
        await cache.get("k")
    assert len(events(logs, LogEvent.CACHE_UNAVAILABLE)) == 2


@pytest.mark.asyncio
async def test_hits_and_misses_log_at_debug(cache: ResponseCache) -> None:
    """Per-request events stay at debug so they do not drown the info log."""
    await cache.set("k", "v", ttl=5)
    with capture_logs() as logs:
        await cache.get("k")
        await cache.get("missing")
    assert {entry["log_level"] for entry in logs} == {"debug"}
