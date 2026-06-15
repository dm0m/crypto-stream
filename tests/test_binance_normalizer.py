from decimal import Decimal

import pytest

from domain.enums import Side
from domain.trade import Trade
from ingestion.normalizers.binance import BinanceNormalizer
from schemas.binance import BinanceTradeRaw


@pytest.fixture
def raw_trade() -> BinanceTradeRaw:
    return {
        "e": "trade",          # event type — the gate the normalizer checks first
        "E": 1718064000123,    # event time (ms epoch)
        "s": "BTCUSDT",        # raw symbol, no separator
        "t": 123456789,        # trade id — note: int on the wire
        "p": "63468.98000000", # price — a STRING, deliberately (→ Decimal)
        "q": "0.00158000",     # quantity — also a string
        "T": 1718064000123,    # trade time (ms epoch) = 2024-06-11T00:00:00.123Z
        "m": False,            # buyer is NOT maker → maps to BUY
        "M": True,             # legacy field, Binance docs say "ignore"
    }


@pytest.fixture
def maker_raw_trade(raw_trade: BinanceTradeRaw) -> BinanceTradeRaw:
    return {**raw_trade, "m": True}


@pytest.fixture
def malformed_raw_trade(raw_trade: BinanceTradeRaw) -> BinanceTradeRaw:
    return {**raw_trade, "p": "not-a-number"}


def test_normalize_returns_trade_for_valid_payload(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert isinstance(trade, Trade), f"expected a Trade, got {type(trade).__name__}: {trade!r}"


def test_buyer_is_maker_maps_to_sell(maker_raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(maker_raw_trade)
    assert trade is not None, "normalize returned None for a valid maker trade"
    assert trade.side is Side.SELL, f"buyer-is-maker should map to SELL, got {trade.side}"


def test_buyer_not_maker_maps_to_buy(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid taker trade"
    assert trade.side is Side.BUY, f"buyer-not-maker should map to BUY, got {trade.side}"


def test_symbol_is_split_into_base_and_quote(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert trade.symbol == "BTC-USDT", f"expected 'BTC-USDT', got {trade.symbol!r}"


def test_price_is_decimal_not_float(raw_trade: BinanceTradeRaw) -> None:
    trade = BinanceNormalizer.normalize(raw_trade)
    assert trade is not None, "normalize returned None for a valid payload"
    assert isinstance(trade.price, Decimal), f"price must be Decimal, got {type(trade.price).__name__}"
    assert trade.price == Decimal("63468.98000000"), f"price corrupted in parsing: {trade.price}"


def test_malformed_payload_returns_none(malformed_raw_trade: BinanceTradeRaw) -> None:
    result = BinanceNormalizer.normalize(malformed_raw_trade)
    assert result is None, f"expected None for an unparseable price, got {result!r}"


def test_malformed_payload_increments_counter(malformed_raw_trade: BinanceTradeRaw) -> None:
    before = BinanceNormalizer.malformed_trade_count
    BinanceNormalizer.normalize(malformed_raw_trade)
    after = BinanceNormalizer.malformed_trade_count
    assert after == before + 1, f"expected counter {before} -> {before + 1}, got {after}"


def test_ack_payload_does_not_increment_counter(raw_trade: BinanceTradeRaw) -> None:
    del raw_trade["e"]  # no "e" = ack, not a malformed trade
    before = BinanceNormalizer.malformed_trade_count
    result = BinanceNormalizer.normalize(raw_trade)
    assert result is None, f"expected None for an ack payload, got {result!r}"
    after = BinanceNormalizer.malformed_trade_count
    assert after == before, f"ack must not increment the malformed counter: {before} -> {after}"
