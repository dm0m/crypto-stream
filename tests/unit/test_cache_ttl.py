"""Unit tests for ``ttl_for``, which picks a response's cache lifetime."""

from datetime import UTC, datetime

import pytest

from api.cache import ttl_for
from api.settings import ApiSettings
from domain.enums import Interval

NOW = datetime(2026, 9, 28, 12, 34, 20, tzinfo=UTC)
SETTINGS = ApiSettings(
    cache_ttl_live_seconds=5, cache_ttl_closed_seconds=600, cache_settle_seconds=60
)
LIVE = SETTINGS.cache_ttl_live_seconds
CLOSED = SETTINGS.cache_ttl_closed_seconds


def at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 28, hour, minute, second, tzinfo=UTC)


def ttl(
    ts_to: datetime,
    cursor: datetime | None = None,
    interval: Interval = Interval.M1,
) -> int:
    return ttl_for(
        ts_to=ts_to, cursor=cursor, interval=interval, now=NOW, settings=SETTINGS
    )


@pytest.mark.parametrize(
    ("ts_to", "expected", "reason"),
    [
        (at(12, 35), LIVE, "includes the open 12:34 candle"),
        (at(13, 0), LIVE, "window reaches into the future"),
        (at(12, 34), LIVE, "newest candle 12:33 closed 20s ago, inside the margin"),
        (at(12, 33, 10), LIVE, "ends 10s into the 12:33 candle, which ended 20s ago"),
        (at(12, 33), CLOSED, "newest candle 12:32 ended 80s ago, past the margin"),
        (at(12, 0), CLOSED, "well in the past"),
    ],
)
def test_minute_windows(ts_to: datetime, expected: int, reason: str) -> None:
    assert ttl(ts_to) == expected, reason


def test_later_page_of_a_live_window_is_cached_as_closed() -> None:
    """A page ends at its cursor, so page five of "the last hour" is history."""
    assert ttl(at(12, 35)) == LIVE
    assert ttl(at(12, 35), cursor=at(12, 20)) == CLOSED


def test_cursor_inside_the_margin_is_still_live() -> None:
    assert ttl(at(12, 35), cursor=at(12, 34)) == LIVE


def test_cursor_later_than_ts_to_does_not_extend_the_window() -> None:
    assert ttl(at(12, 0), cursor=at(12, 35)) == CLOSED


@pytest.mark.parametrize(
    ("ts_to", "expected", "reason"),
    [
        (at(13, 0), LIVE, "includes the 12:00 hour, which is still in progress"),
        (at(12, 30), LIVE, "ends inside the open 12:00 hour"),
        (at(12, 0), CLOSED, "newest candle is the 11:00 hour, over for 34 minutes"),
    ],
)
def test_hour_windows(ts_to: datetime, expected: int, reason: str) -> None:
    assert ttl(ts_to, interval=Interval.H1) == expected, reason


def test_zero_margin_settles_a_candle_as_soon_as_it_ends() -> None:
    """With no margin, only the candle in progress counts as changing."""
    no_margin = SETTINGS.model_copy(update={"cache_settle_seconds": 0})

    def ttl_no_margin(ts_to: datetime) -> int:
        return ttl_for(
            ts_to=ts_to,
            cursor=None,
            interval=Interval.M1,
            now=NOW,
            settings=no_margin,
        )

    assert ttl_no_margin(at(12, 35)) == LIVE
    assert ttl_no_margin(at(12, 34)) == CLOSED
