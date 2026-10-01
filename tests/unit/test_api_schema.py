"""Unit tests pinning the API's wire representation of money and time."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from api.pagination import decode_cursor, encode_cursor
from api.schema import (
    CandleOut,
    CandlePageOut,
    CandleQuery,
    SymbolStatsOut,
    UtcDatetime,
    WireModel,
)
from domain.candle import Candle
from domain.enums import Exchange, Interval
from storage.repositories.candles import CandlePage

TS_OPEN = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)


class Sample(WireModel):
    """Minimal model exercising both conventions in isolation."""

    price: Decimal
    ts: UtcDatetime


def make_candle(**overrides: object) -> CandleOut:
    """Build a valid ``CandleOut``, overriding individual fields by keyword."""
    fields: dict[str, object] = {
        "exchange": Exchange.BINANCE,
        "symbol": "BTC-USDT",
        "interval": Interval.M1,
        "ts_open": TS_OPEN,
        "ts_close": TS_OPEN + timedelta(minutes=1),
        "open": Decimal("86000.00000000"),
        "high": Decimal("86500.00000000"),
        "low": Decimal("85900.00000000"),
        "close": Decimal("86123.45000000"),
        "volume": Decimal("12.34567890"),
        "trade_count": 789,
    }
    return CandleOut.model_validate(fields | overrides)


def test_decimal_serializes_as_a_json_string() -> None:
    """Money must not become a JSON number, which clients parse as a float."""
    payload = Sample(price=Decimal("86123.45000000"), ts=TS_OPEN).model_dump_json()
    assert '"price":"86123.45000000"' in payload


def test_decimal_keeps_trailing_zeros() -> None:
    assert (
        Sample(price=Decimal("1.10"), ts=TS_OPEN)
        .model_dump_json()
        .startswith('{"price":"1.10"')
    )


def test_utc_timestamp_serializes_with_a_z_suffix() -> None:
    """RFC 3339 in UTC, not an offset, so every response reads the same way."""
    payload = Sample(price=Decimal("1"), ts=TS_OPEN).model_dump_json()
    assert '"ts":"2026-09-15T12:00:00Z"' in payload


def test_other_offsets_are_normalized_to_utc() -> None:
    """A caller's offset is accepted but never echoed back."""
    berlin = datetime(2026, 9, 15, 14, 0, tzinfo=timezone(timedelta(hours=2)))
    model = Sample(price=Decimal("1"), ts=berlin)
    assert model.ts == TS_OPEN
    assert '"ts":"2026-09-15T12:00:00Z"' in model.model_dump_json()


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Sample(price=Decimal("1"), ts=datetime(2026, 9, 15, 12, 0))


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Sample.model_validate({"price": "1", "ts": TS_OPEN, "prcie": "2"})


def test_candle_out_carries_every_price_as_a_string() -> None:
    """The whole bar, not just one field, has to obey the money rule."""
    payload = make_candle().model_dump_json()
    for field in ("open", "high", "low", "close", "volume"):
        assert f'"{field}":"' in payload


def test_candle_out_renders_enums_as_their_values() -> None:
    """Clients see ``binance`` and ``1m``, not ``BINANCE`` and ``M1``."""
    payload = make_candle().model_dump_json()
    assert '"exchange":"binance"' in payload
    assert '"interval":"1m"' in payload


def test_candle_out_accepts_a_domain_candle_dump() -> None:
    """The repository's domain objects must map onto the wire model as they are."""
    candle = make_candle()
    assert CandleOut.model_validate(candle.model_dump()) == candle


# --- request model: CandleQuery -------------------------------------------

QUERY = {
    "exchange": "binance",
    "symbol": "BTC-USDT",
    "ts_from": "2026-09-15T12:00:00Z",
    "ts_to": "2026-09-15T13:00:00Z",
}


def test_query_defaults_interval_limit_and_cursor() -> None:
    """A first-page request needs only the venue, pair and window."""
    query = CandleQuery.model_validate(QUERY)
    assert query.interval is Interval.M1
    assert query.cursor is None
    assert query.limit >= 1


@pytest.mark.parametrize(
    "symbol", ["btc-usdt", "BTCUSDT", "BTC/USDT", "BTC-", "-USDT", "B-USDT"]
)
def test_query_rejects_malformed_symbols(symbol: str) -> None:
    with pytest.raises(ValidationError, match="symbol"):
        CandleQuery.model_validate(QUERY | {"symbol": symbol})


@pytest.mark.parametrize(
    "ts_to", ["2026-09-15T12:00:00Z", "2026-09-15T11:00:00Z"], ids=["empty", "reversed"]
)
def test_query_rejects_windows_that_cover_no_time(ts_to: str) -> None:
    with pytest.raises(ValidationError, match="ts_from must be earlier"):
        CandleQuery.model_validate(QUERY | {"ts_to": ts_to})


@pytest.mark.parametrize("limit", [0, -1, 10**9])
def test_query_rejects_out_of_range_limits(limit: int) -> None:
    with pytest.raises(ValidationError, match="limit"):
        CandleQuery.model_validate(QUERY | {"limit": limit})


def test_query_decodes_the_cursor_clients_send_back() -> None:
    """The encoded string from ``next_cursor`` must come back as a timestamp."""
    position = TS_OPEN + timedelta(minutes=30)
    query = CandleQuery.model_validate(QUERY | {"cursor": encode_cursor(position)})
    assert query.cursor == position


def test_query_rejects_a_malformed_cursor() -> None:
    with pytest.raises(ValidationError, match="cursor"):
        CandleQuery.model_validate(QUERY | {"cursor": "not-base64!!"})


def test_query_rejects_unknown_parameters() -> None:
    with pytest.raises(ValidationError):
        CandleQuery.model_validate(QUERY | {"intervall": "1h"})


# --- response models ------------------------------------------------------


def domain_candle(minute: int) -> Candle:
    return Candle(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        interval=Interval.M1,
        ts_open=TS_OPEN + timedelta(minutes=minute),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal("2.5"),
        trade_count=3,
    )


def test_page_out_carries_ts_close_from_the_domain_property() -> None:
    """``ts_close`` is a property, absent from ``model_dump()``; it must still appear."""
    out = CandlePageOut.from_page(CandlePage([domain_candle(0)], None))
    assert out.data[0].ts_close == TS_OPEN + timedelta(minutes=1)


def test_page_out_encodes_the_cursor_as_an_opaque_string() -> None:
    """Storage hands over a ``datetime``; clients must receive the token."""
    position = TS_OPEN + timedelta(minutes=1)
    out = CandlePageOut.from_page(CandlePage([domain_candle(1)], position))
    assert out.next_cursor == encode_cursor(position)
    assert decode_cursor(out.next_cursor) == position


def test_page_out_last_page_has_null_cursor() -> None:
    """``null`` is how a client knows to stop."""
    out = CandlePageOut.from_page(CandlePage([], None))
    assert '"next_cursor":null' in out.model_dump_json()


def test_stats_out_serializes_money_and_time_by_the_wire_rules() -> None:
    """The stats response obeys the same string-money, UTC-Z rules as candles."""
    stats = SymbolStatsOut(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        ts_from=TS_OPEN,
        ts_to=TS_OPEN + timedelta(hours=24),
        as_of=TS_OPEN + timedelta(hours=23, minutes=59),
        last_price=Decimal("84605.03"),
        volume=Decimal("445.65204"),
        vwap=Decimal("84618.2860624129533885"),
    )
    payload = stats.model_dump_json()
    assert '"vwap":"84618.2860624129533885"' in payload
    assert '"as_of":"2026-09-16T11:59:00Z"' in payload
