from enum import StrEnum


class Exchange(StrEnum):
    """Supported exchanges: the canonical domain identifier used across all
    layers (ingestion, storage, api). Exchange-specific *wire* types live in
    ``ingestion/schemas/``; this shared enum belongs to the domain.
    """

    BINANCE = "binance"
    KRAKEN = "kraken"
    COINBASE = "coinbase"


class Side(StrEnum):
    """Direction of a trade from the taker's (aggressor's) point of view."""

    BUY = "buy"
    SELL = "sell"
