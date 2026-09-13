"""Wire-format types for Binance WebSocket payloads."""

from typing import NotRequired, TypedDict


class BinanceTradeRaw(TypedDict):
    """One decoded message from a Binance ``<symbol>@trade`` stream."""

    e: NotRequired[str]
    E: int
    s: str
    t: int
    p: str
    q: str
    T: int
    m: bool
    M: bool
