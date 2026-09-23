from datetime import datetime
from decimal import Decimal

from sqlalchemy import DateTime, Enum, Index, Integer, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from domain.enums import Exchange, Interval, Side


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class TradeTable(Base):
    """Row mapping for the ``trades`` table: one raw tick per row."""

    __tablename__ = "trades"
    ts_event: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), primary_key=True
    )
    exchange: Mapped[Exchange] = mapped_column(Enum(Exchange), primary_key=True)
    trade_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    price: Mapped[Decimal] = mapped_column(Numeric)
    quantity: Mapped[Decimal] = mapped_column(Numeric)
    ts_ingest: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    side: Mapped[Side] = mapped_column(Enum(Side))
    symbol: Mapped[str] = mapped_column(String(20), index=True)
    __table_args__ = (Index("trades_ts_event_idx", ts_event.desc()),)


class CandleTable(Base):
    """Row mapping for the ``candles`` table: one closed OHLCV bar per row."""

    __tablename__ = "candles"
    ts_open: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    exchange: Mapped[Exchange] = mapped_column(Enum(Exchange), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), primary_key=True)
    interval: Mapped[Interval] = mapped_column(
        Enum(Interval, name="candle_interval"), primary_key=True
    )
    open: Mapped[Decimal] = mapped_column(Numeric)
    high: Mapped[Decimal] = mapped_column(Numeric)
    low: Mapped[Decimal] = mapped_column(Numeric)
    close: Mapped[Decimal] = mapped_column(Numeric)
    volume: Mapped[Decimal] = mapped_column(Numeric)
    trade_count: Mapped[int] = mapped_column(Integer)
