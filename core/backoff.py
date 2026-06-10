import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from core.log_events import LogEvent

logger: structlog.BoundLogger = structlog.get_logger()

AsyncFn = Callable[..., Awaitable[None]]


def backoff(
    delay: int = 2,
    healthy_connection_time: int = 60,
) -> Callable[[AsyncFn], AsyncFn]:
    """Retry an async function forever with exponential backoff."""

    def decorator(func: AsyncFn) -> AsyncFn:
        async def wrapper(*args: Any, **kwargs: Any) -> None:
            current_delay = delay
            attempt = 0
            while True:
                start: float = time.perf_counter()
                try:
                    await func(*args, **kwargs)
                    current_delay = delay
                except Exception as e:
                    elapsed: float = time.perf_counter() - start
                    if elapsed >= healthy_connection_time:
                        current_delay = delay
                        attempt = 0
                    else:
                        logger.warning(
                            LogEvent.WS_RECONNECTING,
                            attempt=attempt,
                            delay=current_delay,
                            reason=e.__class__.__name__,
                        )
                        await asyncio.sleep(current_delay)
                        attempt += 1
                        current_delay *= 2

        return wrapper

    return decorator
