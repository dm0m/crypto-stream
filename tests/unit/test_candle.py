"""Unit tests for the ``Candle`` domain model and the ``Interval`` enum."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from domain.candle import Candle
from domain.enums import Exchange, Interval

# A boundary-aligned bucket start every test can build on.
TS_OPEN = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


@pytest.fixture
def candle_kwargs() -> dict[str, Any]:
    return {
        "exchange": Exchange.BINANCE,
        "symbol": "BTC-USDT",
        "interval": Interval.M1,
        "ts_open": TS_OPEN,
        "open": Decimal("100"),
        "high": Decimal("110"),
        "low": Decimal("90"),
        "close": Decimal("105"),
        "volume": Decimal("1.5"),
        "trade_count": 3,
    }


def first_error_message(exc_info: pytest.ExceptionInfo[ValidationError]) -> str:
    return str(exc_info.value.errors()[0]["msg"])


# ---------------------------------------------------------------------------
# Interval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("interval", "expected"),
    [
        (Interval.M1, timedelta(minutes=1)),
        (Interval.H1, timedelta(hours=1)),
    ],
)
def test_duration_matches_member(interval: Interval, expected: timedelta) -> None:
    assert interval.duration == expected


def test_floor_drops_seconds_and_microseconds_for_1m() -> None:
    """12:00:59.999999 belongs to the 12:00 bucket."""
    ts = TS_OPEN + timedelta(seconds=59, microseconds=999_999)
    assert Interval.M1.floor(ts) == TS_OPEN


def test_floor_drops_minutes_for_1h() -> None:
    """12:37:15 belongs to the 12:00 bucket."""
    ts = TS_OPEN + timedelta(minutes=37, seconds=15)
    assert Interval.H1.floor(ts) == TS_OPEN


def test_floor_is_identity_on_aligned_timestamp() -> None:
    assert Interval.M1.floor(TS_OPEN) == TS_OPEN
    assert Interval.H1.floor(TS_OPEN) == TS_OPEN


def test_floor_last_microsecond_before_boundary_stays_in_previous_bucket() -> None:
    """11:59:59.999999 floors to 11:59, not 12:00; the boundary is exclusive."""
    ts = TS_OPEN - timedelta(microseconds=1)
    assert Interval.M1.floor(ts) == TS_OPEN - timedelta(minutes=1)
    assert Interval.H1.floor(ts) == TS_OPEN - timedelta(hours=1)


def test_floor_normalizes_other_timezones_to_utc() -> None:
    """A +02:00 timestamp floors to the same instant expressed in UTC."""
    paris = datetime(2026, 9, 15, 14, 30, 59, tzinfo=timezone(timedelta(hours=2)))
    floored = Interval.M1.floor(paris)
    assert floored == datetime(2026, 9, 15, 12, 30, tzinfo=UTC)
    assert floored.tzinfo is UTC


def test_floor_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="naive"):
        Interval.M1.floor(datetime(2026, 9, 15, 12, 0))


# ---------------------------------------------------------------------------
# Candle: construction and derived values
# ---------------------------------------------------------------------------


def test_valid_candle_constructs(candle_kwargs: dict[str, Any]) -> None:
    candle = Candle(**candle_kwargs)
    assert candle.ts_open == TS_OPEN
    assert candle.open == Decimal("100")
    assert candle.trade_count == 3


def test_ts_close_is_ts_open_plus_duration(candle_kwargs: dict[str, Any]) -> None:
    assert Candle(**candle_kwargs).ts_close == TS_OPEN + timedelta(minutes=1)
    hourly = Candle(**{**candle_kwargs, "interval": Interval.H1})
    assert hourly.ts_close == TS_OPEN + timedelta(hours=1)


def test_candle_is_frozen(candle_kwargs: dict[str, Any]) -> None:
    """Assigning to a field raises, as with ``Trade``."""
    candle = Candle(**candle_kwargs)
    with pytest.raises(ValidationError):
        candle.close = Decimal("1")  # type: ignore[misc]


def test_open_may_equal_close_and_bounds(candle_kwargs: dict[str, Any]) -> None:
    """A single-trade candle has open == high == low == close; must be valid."""
    flat = {
        **candle_kwargs,
        "open": Decimal("100"),
        "high": Decimal("100"),
        "low": Decimal("100"),
        "close": Decimal("100"),
        "trade_count": 1,
    }
    assert Candle(**flat).high == Decimal("100")


# ---------------------------------------------------------------------------
# Candle: alignment validator
# ---------------------------------------------------------------------------


def test_rejects_ts_open_with_stray_seconds(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        Candle(**{**candle_kwargs, "ts_open": TS_OPEN + timedelta(seconds=1)})
    assert "not aligned" in first_error_message(exc_info)


def test_rejects_ts_open_with_stray_microseconds(
    candle_kwargs: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError, match="not aligned"):
        Candle(**{**candle_kwargs, "ts_open": TS_OPEN + timedelta(microseconds=1)})


def test_rejects_1h_candle_opening_mid_hour(candle_kwargs: dict[str, Any]) -> None:
    """12:30 is aligned for 1m but not for 1h."""
    half_past = TS_OPEN + timedelta(minutes=30)
    Candle(**{**candle_kwargs, "ts_open": half_past})  # 1m: fine
    with pytest.raises(ValidationError, match="not aligned to 1h"):
        Candle(**{**candle_kwargs, "ts_open": half_past, "interval": Interval.H1})


def test_rejects_naive_ts_open(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError) as exc_info:
        Candle(**{**candle_kwargs, "ts_open": datetime(2026, 9, 15, 12, 0)})
    assert "timezone" in first_error_message(exc_info)


def test_accepts_aligned_ts_open_in_non_utc_zone(
    candle_kwargs: dict[str, Any],
) -> None:
    """14:00+02:00 is 12:00 UTC, so it is on a 1h boundary."""
    paris_2pm = datetime(2026, 9, 15, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    candle = Candle(**{**candle_kwargs, "ts_open": paris_2pm, "interval": Interval.H1})
    assert candle.ts_open == TS_OPEN


# ---------------------------------------------------------------------------
# Candle: OHLC validator
# ---------------------------------------------------------------------------


def test_rejects_low_above_open(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="low"):
        Candle(**{**candle_kwargs, "low": Decimal("101")})


def test_rejects_low_above_close(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="low"):
        Candle(**{**candle_kwargs, "close": Decimal("89")})


def test_rejects_high_below_open(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="high"):
        Candle(**{**candle_kwargs, "high": Decimal("99")})


def test_rejects_high_below_close(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="high"):
        Candle(**{**candle_kwargs, "close": Decimal("111")})


def test_error_message_names_offending_values(
    candle_kwargs: dict[str, Any],
) -> None:
    """The message should quote the actual low/open/close so failures are debuggable."""
    with pytest.raises(ValidationError) as exc_info:
        Candle(**{**candle_kwargs, "low": Decimal("101")})
    message = first_error_message(exc_info)
    assert "101" in message
    assert "100" in message
    assert "105" in message


# ---------------------------------------------------------------------------
# Candle: field constraints
# ---------------------------------------------------------------------------


def test_rejects_negative_volume(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Candle(**{**candle_kwargs, "volume": Decimal("-0.1")})


def test_accepts_zero_volume(candle_kwargs: dict[str, Any]) -> None:
    assert Candle(**{**candle_kwargs, "volume": Decimal("0")}).volume == 0


def test_rejects_zero_trade_count(candle_kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Candle(**{**candle_kwargs, "trade_count": 0})


def test_string_price_is_parsed_exactly(candle_kwargs: dict[str, Any]) -> None:
    """Strings from JSON land as exact Decimals with no binary rounding."""
    candle = Candle(**{**candle_kwargs, "open": "100.00000001"})
    assert candle.open == Decimal("100.00000001")
    assert str(candle.open) == "100.00000001"
