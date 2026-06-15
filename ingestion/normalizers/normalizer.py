
from typing import Protocol

from domain.trade import Trade
from schemas.binance import BinanceTradeRaw


class Normalizer(Protocol):
    def normalize(self, raw: BinanceTradeRaw) -> Trade | None: ...