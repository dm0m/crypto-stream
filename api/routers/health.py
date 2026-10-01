import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal

import structlog
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core.log_events import LogEvent
from core.redis_client import check_redis_con
from storage.engine import check_db_conn

router = APIRouter(tags=["health"])
logger: structlog.BoundLogger = structlog.get_logger()
CHECKS: dict[str, Callable[[], Awaitable[None]]] = {
    "redis": check_redis_con,
    "postgres": check_db_conn,
}


async def probe(
    name: str, func: Callable[[], Awaitable[None]]
) -> Literal["ok", "unavailable"]:
    try:
        async with asyncio.timeout(1):
            await func()
    except Exception as exc:
        logger.warning(
            LogEvent.HEALTH_CHECK_FAILED, dependency=name, error=type(exc).__name__
        )
        return "unavailable"
    return "ok"


@router.get("/health")
async def health() -> JSONResponse:
    availability = await asyncio.gather(
        *(probe(name, func) for name, func in CHECKS.items())
    )
    return JSONResponse(
        content=dict(zip(CHECKS, availability, strict=True)),
        status_code=503 if "unavailable" in availability else 200,
    )
