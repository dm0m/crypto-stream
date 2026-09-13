from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

from domain.enums import Exchange, Side


class Trade(BaseModel):
    """One executed trade, in the exchange-agnostic shape the whole system uses."""

    trade_id: str
    exchange: Exchange
    symbol: str
    price: Decimal
    quantity: Decimal
    side: Side
    ts_event: datetime
    ts_ingest: datetime

    model_config = ConfigDict(frozen=True)
