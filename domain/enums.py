from datetime import UTC, datetime, timedelta
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


class Interval(StrEnum):
    """Candle bucket width."""

    M1 = "1m"
    H1 = "1h"

    @property
    def duration(self) -> timedelta:
        match self:
            case Interval.M1:
                return timedelta(minutes=1)
            case Interval.H1:
                return timedelta(hours=1)

    def floor(self, ts: datetime) -> datetime:
        """Return the UTC start of the bucket containing ``ts``."""
        if ts.tzinfo is None:
            raise ValueError("cannot floor a naive datetime; pass a tz-aware value")
        seconds = int(self.duration.total_seconds())
        epoch = int(ts.timestamp())
        return datetime.fromtimestamp(epoch - epoch % seconds, tz=UTC)
