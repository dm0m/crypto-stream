"""Streaming OHLCV aggregation for the processing worker."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import structlog

from core.log_events import LogEvent
from domain.candle import Candle
from domain.enums import Exchange, Interval
from domain.trade import Trade

# One aggregation series: the tuple the open-bucket table is keyed on. A plain
# tuple rather than a dataclass so it hashes cheaply and prints readably in
# logs. ``interval`` is part of the key even though an aggregator instance
# handles a single interval, so several instances can share one key space.
SeriesKey = tuple[Exchange, str, Interval]


@dataclass(slots=True)
class _OpenCandle:
    """Mutable scratch state for the bucket currently being built."""

    ts_open: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int

    @classmethod
    def from_trade(cls, trade: Trade, ts_open: datetime) -> "_OpenCandle":
        return cls(
            ts_open=ts_open,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.quantity,
            trade_count=1,
        )

    def update(self, trade: Trade) -> None:
        """Fold one more trade from the same bucket into the running values."""
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        self.close = trade.price
        self.volume += trade.quantity
        self.trade_count += 1

    def to_candle(self, exchange: Exchange, symbol: str, interval: Interval) -> Candle:
        return Candle(
            exchange=exchange,
            symbol=symbol,
            interval=interval,
            ts_open=self.ts_open,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count,
        )


class CandleAggregator:
    """Rolling OHLCV aggregator for one interval across any number of series."""

    def __init__(self, interval: Interval, grace: timedelta) -> None:
        self._interval = interval
        self._grace = grace
        self._open: dict[SeriesKey, _OpenCandle] = {}
        self._late_trades = 0
        self.logger: structlog.BoundLogger = structlog.get_logger().bind(
            component="aggregator", interval=str(interval)
        )

    @property
    def interval(self) -> Interval:
        return self._interval

    @property
    def late_trades(self) -> int:
        return self._late_trades

    @property
    def open_series(self) -> int:
        return len(self._open)

    def add(self, trade: Trade) -> Candle | None:
        """Fold ``trade`` into its bucket and return the candle it closed, if any."""
        bucket = self._interval.floor(trade.ts_event)
        key: SeriesKey = (trade.exchange, trade.symbol, self._interval)
        current = self._open.get(key)

        if current is None:
            self._open[key] = _OpenCandle.from_trade(trade, bucket)
            return None

        if bucket == current.ts_open:
            current.update(trade)
            return None

        if bucket > current.ts_open:
            closed = self._close(key, current)
            self._open[key] = _OpenCandle.from_trade(trade, bucket)
            return closed

        self._late_trades += 1
        self.logger.warning(
            LogEvent.AGGREGATOR_LATE_TRADE,
            exchange=trade.exchange,
            symbol=trade.symbol,
            trade_id=trade.trade_id,
            bucket=bucket.isoformat(),
            open_bucket=current.ts_open.isoformat(),
            lag_ms=int((current.ts_open - bucket).total_seconds() * 1000),
        )
        return None

    def flush_expired(self, now: datetime) -> list[Candle]:
        """Close every bucket whose ``ts_close + grace`` is at or before ``now``."""
        deadline_offset = self._interval.duration + self._grace
        closed: list[Candle] = []
        for key, current in list(self._open.items()):
            if now >= current.ts_open + deadline_offset:
                closed.append(self._close(key, current))
                del self._open[key]
        return closed

    def _close(self, key: SeriesKey, current: _OpenCandle) -> Candle:
        """Freeze ``current`` into a ``Candle`` and log the close."""
        exchange, symbol, interval = key
        candle = current.to_candle(exchange, symbol, interval)
        self.logger.info(
            LogEvent.AGGREGATOR_CANDLE_CLOSED,
            exchange=exchange,
            symbol=symbol,
            ts_open=candle.ts_open.isoformat(),
            close=str(candle.close),
            volume=str(candle.volume),
            trade_count=candle.trade_count,
        )
        return candle
