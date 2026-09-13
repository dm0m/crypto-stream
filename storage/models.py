from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, Enum, Numeric, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from domain.enums import Exchange, Side


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class TradeTable(Base):
    """Row mapping for the ``trades`` table: one raw tick per row."""

    __tablename__ = "trades"
    __table_args__ = (UniqueConstraint("exchange", "trade_id"),)
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    price: Mapped[Decimal] = mapped_column(Numeric)
    quantity: Mapped[Decimal] = mapped_column(Numeric)
    ts_event: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ts_ingest: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    exchange: Mapped[Exchange] = mapped_column(Enum(Exchange))
    trade_id: Mapped[str] = mapped_column(String(20), index=True)
    side: Mapped[Side] = mapped_column(Enum(Side))
    symbol: Mapped[str] = mapped_column(String(20), index=True)
