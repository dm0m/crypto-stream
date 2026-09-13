import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal

from domain.enums import Exchange, Side
from domain.trade import Trade
from processing.worker import StreamEntry


def make_trade() -> Trade:
    return Trade(
        trade_id="123456789",
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT",
        price=Decimal("63468.98"),
        quantity=Decimal("0.00158"),
        side=Side.BUY,
        ts_event=datetime(2026, 6, 11, tzinfo=UTC),
        ts_ingest=datetime(2026, 6, 11, tzinfo=UTC),
    )


def make_trades(count: int) -> list[Trade]:
    return [
        Trade(
            trade_id=f"{i}",
            exchange=Exchange.BINANCE,
            symbol="BTC-USDT",
            price=Decimal("63468.98"),
            quantity=Decimal("0.00158"),
            side=Side.BUY,
            ts_event=datetime(2026, 6, 11, tzinfo=UTC),
            ts_ingest=datetime(2026, 6, 11, tzinfo=UTC),
        )
        for i in range(count)
    ]


def make_entry(entry_id: str = "1-0") -> StreamEntry:
    return entry_id, {"data": make_trade().model_dump_json()}


async def set_shutdown_event_after_delay(delay: float, event: asyncio.Event) -> None:
    """Set `event` after `delay` seconds, stopping any worker watching it."""
    await asyncio.sleep(delay)
    event.set()


async def set_shutdown_event_when_drained(
    event: asyncio.Event,
    is_drained: Callable[[], bool],
    poll_interval: float = 0.05,
) -> None:
    try:
        while not is_drained():  # noqa: ASYNC110  (predicate over counters, no event to await)
            await asyncio.sleep(poll_interval)
    finally:
        event.set()
