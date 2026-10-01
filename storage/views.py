"""Core constructs for the continuous aggregates ``candles_1m`` and ``candles_1h``."""

from typing import Any

from sqlalchemy import (
    ColumnClause,
    DateTime,
    Enum,
    TableClause,
    column,
    table,
)

from domain.enums import Exchange, Interval


def _columns() -> tuple[ColumnClause[Any], ...]:
    return (
        column("ts_open", DateTime(timezone=True)),
        column("exchange", Enum(Exchange)),
        column("symbol"),
        column("open"),
        column("high"),
        column("low"),
        column("close"),
        column("volume"),
        column("trade_count"),
        column("quote_volume"),
    )


candles_1m = table("candles_1m", *_columns())
candles_1h = table("candles_1h", *_columns())


def view_for(interval: Interval) -> TableClause:
    match interval:
        case Interval.M1:
            return candles_1m
        case Interval.H1:
            return candles_1h
