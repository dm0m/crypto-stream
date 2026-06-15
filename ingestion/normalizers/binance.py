from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from pydantic import ValidationError
import structlog

from core.log_events import LogEvent
from domain.enums import Exchange, Side
from domain.trade import Trade
from schemas.binance import BinanceTradeRaw


class BinanceNormalizer():
    
    logger: structlog.BoundLogger = structlog.get_logger()
    malformed_trade_count: int = 0
    
    BINANCE_QUOTE_ASSETS: set[str] = {
        "BTC", "ETH", "USDT", "BNB", "TUSD", "PAX", "USDC", "XRP", "USDS",
        "TRX", "BUSD", "NGN", "RUB", "TRY", "EUR", "ZAR", "BKRW", "IDRT",
        "GBP", "UAH", "BIDR", "AUD", "DAI", "BRL", "BVND", "VAI", "USDP",
        "DOGE", "UST", "DOT", "PLN", "RON", "ARS"
    }
    
    @classmethod
    def normalize(cls, raw: BinanceTradeRaw) -> Trade | None:
        if raw.get("e") != "trade": # probably ack event: {"result":null,"id":1}
            cls.logger.debug(LogEvent.WS_ACK_RECEIVED, ack=raw)
            return None
        
        try:
            return Trade(
                trade_id=str(raw["t"]),
                exchange=Exchange.BINANCE,
                symbol=cls._to_symbol(raw["s"]),
                price=Decimal(raw["p"]),
                quantity=Decimal(raw["q"]),
                side=Side.SELL if raw["m"] else Side.BUY,
                ts_event=datetime.fromtimestamp(raw["T"] / 1000, tz=timezone.utc),
                ts_ingest=datetime.now(timezone.utc)
            )
        except (InvalidOperation, KeyError, ValidationError) as e:
            # TODO: rate-limit
            cls.malformed_trade_count += 1
            cls.logger.warning(LogEvent.TRADE_DROPPED, error=str(e), raw=raw)
            return None
             
    
    @classmethod
    def _to_symbol(cls, symbol: str) -> str:
        for quote in cls.BINANCE_QUOTE_ASSETS:
           if symbol.endswith(quote):
               base: str = symbol[:-len(quote)]
               return f"{base}-{quote}"
        return symbol
                        
            
            