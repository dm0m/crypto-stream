from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from domain.enums import Exchange, Side

class Trade(BaseModel):
    trade_id: str
    exchange: Exchange
    symbol: str
    price: Decimal
    quantity: Decimal
    side: Side
    ts_event: datetime
    ts_ingest: datetime
    
    model_config = ConfigDict(frozen=True)
