from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from domain.enums import Exchange, Interval


class Candle(BaseModel):
    """One closed OHLCV bar for a symbol on an exchange over a fixed interval."""

    exchange: Exchange
    symbol: str
    interval: Interval
    ts_open: AwareDatetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Field(ge=0)
    trade_count: int = Field(ge=1)

    model_config = ConfigDict(frozen=True)

    @property
    def ts_close(self) -> datetime:
        return self.ts_open + self.interval.duration

    @model_validator(mode="after")
    def check_alignment(self) -> Self:
        """Reject a ``ts_open`` that is not exactly on an interval boundary."""
        if self.interval.floor(self.ts_open) != self.ts_open:
            raise ValueError(
                f"ts_open {self.ts_open.isoformat()} is not aligned to {self.interval}"
            )
        return self

    @model_validator(mode="after")
    def check_ohlc(self) -> Self:
        """Require ``low <= min(open, close)`` and ``high >= max(open, close)``."""
        if self.low > min(self.open, self.close):
            raise ValueError(
                f"low {self.low} exceeds open {self.open} or close {self.close}"
            )
        if self.high < max(self.open, self.close):
            raise ValueError(
                f"high {self.high} is below open {self.open} or close {self.close}"
            )
        return self
