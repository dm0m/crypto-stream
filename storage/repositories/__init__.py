"""Repositories: the only place SQL lives, one module per table."""

from storage.repositories.candles import CandleRepository
from storage.repositories.trades import TradeRepository

__all__ = ["CandleRepository", "TradeRepository"]
