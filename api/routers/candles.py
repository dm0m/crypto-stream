from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Query

from api.cache import ttl_for
from api.dependencies import CandleRepoDep, ResponseCacheDep, SettingsDep
from api.schema import CandlePageOut, CandleQuery
from api.symbols import ensure_known_symbol

router = APIRouter(tags=["candles"])


@router.get("/candles")
async def get_candles(
    candle_repository: CandleRepoDep,
    candle_query: Annotated[CandleQuery, Query()],
    response_cache: ResponseCacheDep,
    settings: SettingsDep,
) -> CandlePageOut:
    ensure_known_symbol(candle_query.exchange, candle_query.symbol, settings)
    key: str = response_cache.key("candles", query=candle_query)
    cached = await response_cache.get(key)
    if cached is not None:
        return CandlePageOut.model_validate_json(cached)
    page = CandlePageOut.from_page(
        await candle_repository.get_candles(
            exchange=candle_query.exchange,
            symbol=candle_query.symbol,
            interval=candle_query.interval,
            ts_from=candle_query.ts_from,
            ts_to=candle_query.ts_to,
            limit=candle_query.limit,
            cursor=candle_query.cursor,
        )
    )
    ttl = ttl_for(
        ts_to=candle_query.ts_to,
        cursor=candle_query.cursor,
        interval=candle_query.interval,
        now=datetime.now(UTC),
        settings=settings,
    )
    await response_cache.set(key, page.model_dump_json(), ttl)
    return page
