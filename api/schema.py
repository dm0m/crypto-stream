"""Wire representation shared by every API response."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Self

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from api.pagination import decode_cursor, encode_cursor
from api.settings import get_api_settings
from domain.enums import Exchange, Interval
from storage.repositories.candles import CandlePage, SymbolStats


def _to_utc(ts: datetime) -> datetime:
    return ts.astimezone(UTC)


UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]
"""A timestamp that must state its offset and is stored and emitted as UTC."""


class WireModel(BaseModel):
    """Base class for everything crossing the HTTP boundary.

    Exists so the conventions described in this module's docstring are
    inherited rather than repeated, and so a reader can tell at a glance
    whether a model is a wire shape or an internal one. ``extra="forbid"``
    makes an unrecognized field an error instead of silent acceptance, which
    on a request turns a misspelled parameter into a 422 the caller can act
    on, rather than a filter that was quietly ignored.
    """

    model_config = ConfigDict(extra="forbid")


class CandleOut(WireModel):
    """One OHLCV bar as clients see it.

    Mirrors ``domain.Candle`` rather than reusing it, because the two answer
    to different pressures: the domain model enforces invariants (bucket
    alignment, high and low bracketing open and close) that are the
    aggregator's business, while this model owes its stability to clients and
    must not change shape when an invariant is added. ``ts_close`` is included
    although it is derivable, because a client charting a bar should not have
    to know the interval arithmetic to place it.

    Attributes:
        exchange: Venue the trades came from.
        symbol: Normalized pair in ``BASE-QUOTE`` form, e.g. ``BTC-USDT``.
        interval: Width of the bucket this bar covers.
        ts_open: Start of the bucket, inclusive.
        ts_close: End of the bucket, exclusive.
        open: Price of the first trade in the bucket.
        high: Highest price traded in the bucket.
        low: Lowest price traded in the bucket.
        close: Price of the last trade in the bucket.
        volume: Base asset quantity traded in the bucket.
        trade_count: Number of trades the bar aggregates.
    """

    exchange: Exchange
    symbol: str
    interval: Interval
    ts_open: UtcDatetime
    ts_close: UtcDatetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int


class CandlePageOut(WireModel):
    """One page of candles and the cursor to fetch the next one.

    The response to ``GET /candles``. It is an object rather than a bare list
    because the cursor has to travel somewhere: to get the next page, a
    client sends ``next_cursor`` back unchanged as the ``cursor`` query
    parameter. ``next_cursor`` is ``null`` on the last page.

    Attributes:
        data: The candles on this page, newest first.
        next_cursor: Opaque token for the next page, or ``None`` when there
            is none.
    """

    data: list[CandleOut]
    next_cursor: str | None

    @classmethod
    def from_page(cls, page: CandlePage) -> Self:
        return cls(
            data=[
                CandleOut.model_validate(candle, from_attributes=True)
                for candle in page.candles
            ],
            next_cursor=(encode_cursor(page.next_cursor) if page.next_cursor else None),
        )


settings = get_api_settings()

Symbol = Annotated[
    str,
    Field(
        pattern=r"^[A-Z0-9]{2,12}-[A-Z0-9]{2,12}$",
        examples=["BTC-USDT"],
        description="Trading pair in BASE-QUOTE form, upper case.",
    ),
]
"""A normalized trading pair such as ``BTC-USDT``."""


class WindowQuery(WireModel):
    """A half-open time window, ``ts_from <= t < ts_to``, shared by query models."""

    ts_from: UtcDatetime
    ts_to: UtcDatetime

    @model_validator(mode="after")
    def check_window(self) -> Self:
        """Reject a window that ends before it starts, or covers no time."""
        if self.ts_from >= self.ts_to:
            raise ValueError("ts_from must be earlier than ts_to")
        return self


class CandleQuery(WindowQuery):
    """Query parameters of ``GET /candles``.

    Collected into one model so that every rule about a valid request lives
    here and is enforced before the handler runs. Any violation becomes a 422
    that names the offending parameter, and the handler only ever sees a
    request that makes sense.

    The window is half-open: candles with ``ts_from <= ts_open < ts_to``.

    Attributes:
        exchange: Venue to read.
        symbol: Trading pair, e.g. ``BTC-USDT``.
        interval: Candle width; one minute unless stated.
        ts_from: Inclusive start of the window; must carry a UTC offset.
        ts_to: Exclusive end of the window; must carry a UTC offset.
        limit: Maximum candles per page.
        cursor: ``next_cursor`` from the previous page, sent back unchanged;
            omitted for the first page.
    """

    exchange: Exchange
    symbol: Symbol
    interval: Interval = Interval.M1
    ts_from: UtcDatetime
    ts_to: UtcDatetime
    limit: int = Field(settings.default_page_size, ge=1, le=settings.max_page_size)
    cursor: UtcDatetime | None = None

    @field_validator("cursor", mode="before")
    @classmethod
    def decode(cls, value: object) -> object:
        """Turn the opaque cursor string a client sends into a timestamp."""
        if isinstance(value, str):
            return decode_cursor(value)
        return value

    @model_validator(mode="after")
    def check_window(self) -> Self:
        """Reject a window that ends before it starts, or covers no time."""
        if self.ts_from >= self.ts_to:
            raise ValueError("ts_from must be earlier than ts_to")
        return self


class SymbolStatsQuery(WindowQuery):
    exchange: Exchange


class SymbolStatsOut(WireModel):
    """Summary of a symbol's trading over a window, as clients see it.

    The response to ``GET /stats/{symbol}``. The window is stated explicitly
    so a client knows what period "24h" was measured against, and ``as_of``
    says how fresh ``last_price`` is.

    Attributes:
        exchange: Venue the figures come from.
        symbol: Trading pair, e.g. ``BTC-USDT``.
        ts_from: Inclusive start of the window the figures cover.
        ts_to: Exclusive end of that window.
        as_of: Start of the most recent one-minute bucket that had trades;
            ``last_price`` is that bucket's closing price.
        last_price: Most recent traded price in the window.
        volume: Base asset quantity traded in the window.
        vwap: Volume-weighted average price over the window: total money
            traded divided by total quantity traded.
    """

    exchange: Exchange
    symbol: str
    ts_from: UtcDatetime
    ts_to: UtcDatetime
    as_of: UtcDatetime
    last_price: Decimal
    volume: Decimal
    vwap: Decimal

    @classmethod
    def from_symbol_stats(
        cls, stats: SymbolStats, symbol: str, query: SymbolStatsQuery
    ) -> Self:
        return cls(
            exchange=query.exchange,
            symbol=symbol,
            ts_from=query.ts_from,
            ts_to=query.ts_to,
            as_of=stats.as_of,
            last_price=stats.last_price,
            volume=stats.volume,
            vwap=stats.vwap,
        )
