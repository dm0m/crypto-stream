from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import CandleRepoDep, SettingsDep
from api.schema import Symbol, SymbolStatsOut, SymbolStatsQuery
from api.symbols import ensure_known_symbol

router = APIRouter(tags=["stats"])


@router.get("/stats/{symbol}")
async def get_symbol_stats(
    candle_repository: CandleRepoDep,
    query: Annotated[SymbolStatsQuery, Query()],
    symbol: Symbol,
    settings: SettingsDep,
) -> SymbolStatsOut:
    ensure_known_symbol(query.exchange, symbol, settings)
    stats = await candle_repository.get_stats(
        symbol=symbol, exchange=query.exchange, ts_from=query.ts_from, ts_to=query.ts_to
    )
    if stats is None:
        raise HTTPException(
            status_code=404,
            detail=f"no trades for {symbol} on {query.exchange} in that window",
        )
    return SymbolStatsOut.from_symbol_stats(stats, symbol, query)
