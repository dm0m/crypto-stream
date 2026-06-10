from typing import TypedDict

class BinanceTradeRaw(TypedDict):
    e: str
    E: int
    s: str
    t: int
    p: str
    q: str
    T: int
    m: bool
    M: bool