"""Entrypoint for the ingestion service: ``python main.py``."""

import asyncio
import os
import signal

import structlog

from core.log_events import LogEvent
from core.logging import configure_logging
from core.redis_client import check_redis_con, redis_client
from ingestion.service import IngestionService

logger: structlog.BoundLogger = structlog.get_logger().bind(service="ingestion")


async def main() -> None:
    """Verify Redis, install signal handlers, run ingestion, then close Redis."""
    await check_redis_con()
    logger.info(LogEvent.REDIS_CONNECTED)
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
    service = IngestionService(redis_client, shutdown_event)
    await service.run()
    await redis_client.aclose(True)


if __name__ == "__main__":
    configure_logging()
    structlog.contextvars.bind_contextvars(pid=os.getpid())
    asyncio.run(main())
