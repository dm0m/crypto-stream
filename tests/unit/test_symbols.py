"""Unit tests for ``ensure_known_symbol`` and the ``symbols`` setting."""

import pytest
from fastapi import HTTPException

from api.settings import ApiSettings
from api.symbols import ensure_known_symbol
from domain.enums import Exchange

SETTINGS = ApiSettings(
    symbols={
        Exchange.BINANCE: frozenset({"BTC-USDT", "ETH-USDT"}),
        Exchange.KRAKEN: frozenset({"BTC-USD"}),
    }
)


@pytest.mark.parametrize(
    ("exchange", "symbol"),
    [
        (Exchange.BINANCE, "BTC-USDT"),
        (Exchange.BINANCE, "ETH-USDT"),
        (Exchange.KRAKEN, "BTC-USD"),
    ],
)
def test_served_pairs_pass(exchange: Exchange, symbol: str) -> None:
    ensure_known_symbol(exchange, symbol, SETTINGS)


@pytest.mark.parametrize(
    ("exchange", "symbol", "reason"),
    [
        (Exchange.BINANCE, "BTC-UDST", "typo of a served pair"),
        (Exchange.KRAKEN, "BTC-USDT", "served on binance, not on kraken"),
        (Exchange.COINBASE, "BTC-USDT", "exchange with no pairs configured"),
    ],
)
def test_unknown_pairs_are_404(exchange: Exchange, symbol: str, reason: str) -> None:
    """Anything not configured for that exchange is a 404."""
    with pytest.raises(HTTPException) as caught:
        ensure_known_symbol(exchange, symbol, SETTINGS)
    assert caught.value.status_code == 404, reason


def test_404_names_the_pair_and_lists_the_served_ones() -> None:
    """The message should let a client correct the request on its own."""
    with pytest.raises(HTTPException) as caught:
        ensure_known_symbol(Exchange.BINANCE, "BTC-UDST", SETTINGS)
    assert caught.value.detail == (
        "unknown symbol BTC-UDST on binance; served: BTC-USDT, ETH-USDT"
    )


def test_exchange_with_no_pairs_says_none() -> None:
    """An exchange missing from the setting reports that nothing is served."""
    with pytest.raises(HTTPException) as caught:
        ensure_known_symbol(Exchange.COINBASE, "BTC-USDT", SETTINGS)
    assert str(caught.value.detail).endswith("served: none")


def test_default_serves_what_ingestion_collects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ingestion subscribes to btcusdt on Binance, so that is the default."""
    monkeypatch.delenv("API_SYMBOLS", raising=False)
    settings = ApiSettings(_env_file=None)
    assert settings.symbols == {Exchange.BINANCE: frozenset({"BTC-USDT"})}


def test_symbols_parse_from_json_in_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The environment carries the mapping as JSON keyed by exchange value."""
    monkeypatch.setenv("API_SYMBOLS", '{"binance": ["BTC-USDT", "ETH-USDT"]}')
    settings = ApiSettings(_env_file=None)
    assert settings.symbols == {Exchange.BINANCE: frozenset({"BTC-USDT", "ETH-USDT"})}
