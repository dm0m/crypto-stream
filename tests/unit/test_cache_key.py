"""Unit tests for ``ResponseCache.key``."""

from datetime import UTC, datetime
from typing import Any

import pytest
from redis.asyncio import Redis

from api.cache import ResponseCache
from api.pagination import encode_cursor
from api.schema import CandleQuery, SymbolStatsQuery

STATS = {
    "exchange": "binance",
    "ts_from": "2026-09-24T21:54:00Z",
    "ts_to": "2026-09-25T21:55:00Z",
}
CANDLES = STATS | {"symbol": "BTC-USDT"}


@pytest.fixture
def cache() -> ResponseCache:
    return ResponseCache(Redis())


def stats_key(cache: ResponseCache, symbol: str = "BTC-USDT", **overrides: Any) -> str:
    return cache.key(
        "stats", symbol, query=SymbolStatsQuery.model_validate(STATS | overrides)
    )


def candles_key(cache: ResponseCache, **overrides: Any) -> str:
    return cache.key("candles", query=CandleQuery.model_validate(CANDLES | overrides))


# --- the same request must map to the same key ---------------------------------


def test_key_is_deterministic(cache: ResponseCache) -> None:
    assert stats_key(cache) == stats_key(cache)


@pytest.mark.parametrize(
    "ts_from",
    [
        "2026-09-24T21:54:00Z",
        "2026-09-24T21:54:00+00:00",
        "2026-09-24T23:54:00+02:00",
        "2026-09-24T16:54:00-05:00",
    ],
    ids=["z", "plus_zero", "berlin", "new_york"],
)
def test_same_instant_in_any_offset_gives_the_same_key(
    cache: ResponseCache, ts_from: str
) -> None:
    assert stats_key(cache, ts_from=ts_from) == stats_key(cache)


def test_omitted_default_and_explicit_default_give_the_same_key(
    cache: ResponseCache,
) -> None:
    assert candles_key(cache) == candles_key(cache, interval="1m")


# --- different requests must map to different keys -----------------------------


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        ("window end moves one minute", {"ts_to": "2026-09-25T21:56:00Z"}),
        ("window start moves one second", {"ts_from": "2026-09-24T21:54:01Z"}),
        ("different exchange", {"exchange": "kraken"}),
    ],
)
def test_stats_query_changes_change_the_key(
    cache: ResponseCache, label: str, overrides: dict[str, str]
) -> None:
    assert stats_key(cache, **overrides) != stats_key(cache), label


def test_symbol_passed_as_a_part_changes_the_key(cache: ResponseCache) -> None:
    assert stats_key(cache, symbol="ETH-USDT") != stats_key(cache, symbol="BTC-USDT")


@pytest.mark.parametrize(
    ("label", "overrides"),
    [
        ("different symbol", {"symbol": "ETH-USDT"}),
        ("different interval", {"interval": "1h"}),
        ("different page size", {"limit": 7}),
    ],
)
def test_candle_query_changes_change_the_key(
    cache: ResponseCache, label: str, overrides: dict[str, Any]
) -> None:
    assert candles_key(cache, **overrides) != candles_key(cache), label


def test_cursor_changes_the_key(cache: ResponseCache) -> None:
    """Page two of a query is a different response from page one."""
    page_two = encode_cursor(datetime(2026, 9, 25, 12, 0, tzinfo=UTC))
    assert candles_key(cache, cursor=page_two) != candles_key(cache)


def test_endpoint_changes_the_key(cache: ResponseCache) -> None:
    """Two endpoints given identical parameters must not share an entry."""
    query = SymbolStatsQuery.model_validate(STATS)
    assert cache.key("stats", "BTC-USDT", query=query) != cache.key(
        "other", "BTC-USDT", query=query
    )


def test_namespace_version_changes_the_key() -> None:
    """Bumping ``v1`` to ``v2`` is how every old entry is abandoned at once."""
    query = SymbolStatsQuery.model_validate(STATS)
    old = ResponseCache(Redis(), namespace="cs:v1").key(
        "stats", "BTC-USDT", query=query
    )
    new = ResponseCache(Redis(), namespace="cs:v2").key(
        "stats", "BTC-USDT", query=query
    )
    assert old != new


# --- the key's format ----------------------------------------------------------


def test_key_has_a_readable_prefix_and_a_short_digest(cache: ResponseCache) -> None:
    """Readable up to the digest, so keys can be scanned by endpoint and symbol."""
    namespace, version, endpoint, symbol, digest = stats_key(cache).split(":")
    assert (namespace, version, endpoint, symbol) == ("cs", "v1", "stats", "BTC-USDT")
    assert len(digest) == 16
    assert all(c in "0123456789abcdef" for c in digest)
