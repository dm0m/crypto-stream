from typing import Protocol

from domain.trade import Trade
from ingestion.schemas.binance import BinanceTradeRaw


class Normalizer(Protocol):
    """Strategy interface: turn one exchange-specific message into a ``Trade``."""

    def normalize(self, raw: BinanceTradeRaw) -> Trade | None: ...
