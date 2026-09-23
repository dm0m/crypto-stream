"""Entrypoint for the processing service: ``python -m processing``."""

import asyncio
import os
import signal
from datetime import timedelta

import structlog

from core.config import get_settings
from core.log_events import LogEvent
from core.logging import configure_logging
from core.redis_client import check_redis_con, redis_client
from domain.enums import Interval
from processing.aggregator import CandleAggregator
from processing.worker import TradeWorker
from storage.engine import check_db_conn, engine, session_factory
from storage.repositories import CandleRepository, TradeRepository

logger: structlog.BoundLogger = structlog.get_logger().bind(service="processing")


async def main() -> None:
    """Verify Redis, install signal handlers, and run a worker until shutdown."""
    await check_redis_con()
    await check_db_conn()
    logger.info(LogEvent.REDIS_CONNECTED)
    settings = get_settings()
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
    worker = TradeWorker(
        consumer=f"{settings.trade_worker_consumer}_{os.getpid()}",
        recovery_consumer=f"{settings.trade_recovery_consumer}_{os.getpid()}",
        redis_client=redis_client,
        trade_repository=TradeRepository(session_factory),
        shutdown_event=shutdown_event,
        aggregator=CandleAggregator(
            Interval(settings.candle_interval),
            timedelta(seconds=settings.candle_grace_seconds),
        ),
        candle_repository=CandleRepository(session_factory),
    )
    await worker.run()
    await redis_client.aclose(True)
    await engine.dispose()


if __name__ == "__main__":
    configure_logging()
    structlog.contextvars.bind_contextvars(pid=os.getpid())
    asyncio.run(main())
