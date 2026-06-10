from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

class Trade(BaseModel):
    trade_id: str
    exchange: Literal["binance", "kraken", "coinbase"]
    symbol: str
    price: Decimal
    quantity: Decimal
    side: Literal["buy", "sell"]
    ts_event: datetime
    ts_ingest: datetime
    
    model_config = ConfigDict(frozen=True)