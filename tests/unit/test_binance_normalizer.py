from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast

import pytest

from domain.enums import Side
from domain.trade import Trade
from ingestion.normalizers.binance import BinanceNormalizer
from ingestion.schemas.binance import BinanceTradeRaw


@pytest.fixture
def raw_trade() -> BinanceTradeRaw:
    return {
        "e": "trade",  # event type: the gate the normalizer checks first
        "E": 1718064000123,  # event time (ms epoch)
        "s": "BTCUSDT",  # raw symbol, no separator
        "t": 123456789,  # trade id; note: int on the wire
        "p": "63468.98000000",  # price: a STRING, deliberately (parsed to Decimal)
        "q": "0.00158000",  # quantity: also a string
        "T": 1718064000123,  # trade time (ms epoch) = 2024-06-11T00:00:00.123Z
        "m": False,  # buyer is NOT maker, so maps to BUY
        "M": True,  # legacy field, Binance docs say "ignore"
    }


@pytest.fixture
def maker_raw_trade(raw_trade: BinanceTradeRaw) -> BinanceTradeRaw:
    return {**raw_trade, "m": True}


@pytest.fixture
def malformed_raw_trade(raw_trade: BinanceTradeRaw) -> BinanceTradeRaw:
    return {**raw_trade, "p": "not-a-number"}


def test_normalize_returns_trade_for_valid_payload(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert isinstance(trade, Trade), (
        f"expected a Trade, got {type(trade).__name__}: {trade!r}"
    )


def test_buyer_is_maker_maps_to_sell(maker_raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(maker_raw_trade)
    assert trade is not None, "normalize returned None for a valid maker trade"
    assert trade.side is Side.SELL, (
        f"buyer-is-maker should map to SELL, got {trade.side}"
    )


def test_buyer_not_maker_maps_to_buy(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid taker trade"
    assert trade.side is Side.BUY, (
        f"buyer-not-maker should map to BUY, got {trade.side}"
    )


def test_symbol_is_split_into_base_and_quote(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert trade.symbol == "BTC-USDT", f"expected 'BTC-USDT', got {trade.symbol!r}"


def test_price_is_decimal_not_float(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert isinstance(trade.price, Decimal), (
        f"price must be Decimal, got {type(trade.price).__name__}"
    )
    assert trade.price == Decimal("63468.98000000"), (
        f"price corrupted in parsing: {trade.price}"
    )


def test_malformed_payload_returns_none(malformed_raw_trade: BinanceTradeRaw) -> None:
    result = BinanceNormalizer.normalize(malformed_raw_trade)
    assert result is None, f"expected None for an unparseable price, got {result!r}"


def test_malformed_payload_increments_counter(
    malformed_raw_trade: BinanceTradeRaw,
) -> None:
    before = BinanceNormalizer.malformed_trade_count
    BinanceNormalizer.normalize(malformed_raw_trade)
    after = BinanceNormalizer.malformed_trade_count
    assert after == before + 1, (
        f"expected counter {before} -> {before + 1}, got {after}"
    )


def test_ack_payload_does_not_increment_counter(raw_trade: BinanceTradeRaw) -> None:
    del raw_trade["e"]  # no "e" = ack, not a malformed trade
    before = BinanceNormalizer.malformed_trade_count
    result = BinanceNormalizer.normalize(raw_trade)
    assert result is None, f"expected None for an ack payload, got {result!r}"
    after = BinanceNormalizer.malformed_trade_count
    assert after == before, (
        f"ack must not increment the malformed counter: {before} -> {after}"
    )


# --- gaps found in the 2026-09-13 coverage review ---------------------------


def test_ts_event_is_tz_aware_utc_with_ms_precision(raw_trade: BinanceTradeRaw) -> None:
    """``T`` is a ms epoch -> ts_event is that instant in UTC, microseconds intact."""
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert trade.ts_event == datetime(2024, 6, 11, 0, 0, 0, 123000, tzinfo=UTC)
    assert trade.ts_event.utcoffset() == timedelta(0), "ts_event must be tz-aware UTC"
    assert trade.ts_ingest.utcoffset() == timedelta(0), "ts_ingest must be tz-aware UTC"


def test_trade_id_is_the_wire_integer_as_a_string(raw_trade: BinanceTradeRaw) -> None:
    """``t`` arrives as an int; the dedup key stores it as its decimal string."""
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert trade.trade_id == "123456789"


def test_quantity_is_decimal_from_string(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert isinstance(trade.quantity, Decimal)
    assert str(trade.quantity) == "0.00158000"


def test_price_survives_values_a_float_cannot_hold(raw_trade: BinanceTradeRaw) -> None:
    """A 21-significant-digit price is preserved digit for digit."""
    raw = cast(BinanceTradeRaw, {**raw_trade, "p": "12345678.1234567890123"})
    trade = BinanceNormalizer.normalize(raw)
    assert trade is not None, "normalize returned None for a valid payload"
    assert str(trade.price) == "12345678.1234567890123"


@pytest.mark.parametrize("missing_key", ["t", "s", "p", "q", "m", "T"])
def test_missing_key_is_dropped_and_counted(
    raw_trade: BinanceTradeRaw, missing_key: str
) -> None:
    """A trade event lacking a required field -> None, counter +1 (KeyError branch)."""
    raw = cast(
        BinanceTradeRaw, {k: v for k, v in raw_trade.items() if k != missing_key}
    )
    before = BinanceNormalizer.malformed_trade_count
    assert BinanceNormalizer.normalize(raw) is None
    assert BinanceNormalizer.malformed_trade_count == before + 1


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_price_is_dropped_and_counted(
    raw_trade: BinanceTradeRaw, price: str
) -> None:
    """``Decimal`` parses these without error; the ``Trade`` model is what rejects them."""
    raw = cast(BinanceTradeRaw, {**raw_trade, "p": price})
    before = BinanceNormalizer.malformed_trade_count
    assert BinanceNormalizer.normalize(raw) is None
    assert BinanceNormalizer.malformed_trade_count == before + 1


def test_unknown_quote_asset_leaves_symbol_unchanged(
    raw_trade: BinanceTradeRaw,
) -> None:
    """No listed quote matches the suffix -> the raw symbol is passed through, not guessed."""
    raw = cast(BinanceTradeRaw, {**raw_trade, "s": "BTCUSD"})
    trade = BinanceNormalizer.normalize(raw)
    assert trade is not None, "an unrecognized quote must not drop the trade"
    assert trade.symbol == "BTCUSD"


@pytest.mark.parametrize("bad_timestamp", ["abc", None], ids=["str", "none"])
def test_non_numeric_timestamp_is_dropped_and_counted(
    raw_trade: BinanceTradeRaw, bad_timestamp: object
) -> None:
    """``T`` that cannot be divided -> None, counter +1, no exception."""
    raw = cast(BinanceTradeRaw, {**raw_trade, "T": bad_timestamp})
    before = BinanceNormalizer.malformed_trade_count
    assert BinanceNormalizer.normalize(raw) is None
    assert BinanceNormalizer.malformed_trade_count == before + 1


def test_non_object_payload_is_dropped_and_counted() -> None:
    """A JSON array or scalar where an object was expected -> None, counter +1."""
    before = BinanceNormalizer.malformed_trade_count
    assert BinanceNormalizer.normalize(cast(BinanceTradeRaw, [1, 2, 3])) is None
    assert BinanceNormalizer.malformed_trade_count == before + 1
