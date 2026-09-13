"""Retry and shutdown-aware waiting primitives shared by both services."""

import asyncio
import time
from collections.abc import Awaitable, Callable

import structlog
import websockets
from redis.exceptions import AuthenticationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy.exc import DBAPIError

from core.log_events import LogEvent

logger: structlog.BoundLogger = structlog.get_logger()
MAX_BACKOFF_DELAY = 60


async def backoff(
    func: Callable[[], Awaitable[None]],
    *,
    event: LogEvent,
    delay: int = 2,
    healthy_connection_time: int = 60,
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Call ``func`` forever, retrying with exponential backoff on failure."""
    current_delay = delay
    attempt = 0
    while shutdown_event is None or not shutdown_event.is_set():
        start: float = time.perf_counter()
        try:
            await func()
            current_delay = delay
        except (websockets.ConnectionClosed, OSError, TimeoutError) as e:
            if shutdown_event is not None and shutdown_event.is_set():
                break

            elapsed: float = time.perf_counter() - start
            if elapsed >= healthy_connection_time:
                current_delay = delay
                attempt = 0
            else:
                logger.warning(
                    event,
                    attempt=attempt,
                    delay=current_delay,
                    reason=e.__class__.__name__,
                )
                if shutdown_event is None:
                    await asyncio.sleep(current_delay)
                else:
                    sleep_task = asyncio.create_task(asyncio.sleep(current_delay))
                    try:
                        await race_wait_task(sleep_task, shutdown_event)
                    except asyncio.CancelledError as e:
                        break
                attempt += 1
                current_delay = min(current_delay * 2, MAX_BACKOFF_DELAY)


async def retry_call[T](
    coro_fn: Callable[[], Awaitable[T]],
    *,
    event: LogEvent,
    delay: int = 2,
    shutdown_event: asyncio.Event | None = None,
) -> T:
    """Await ``coro_fn`` until it succeeds, retrying transient connection errors.

    Raises:
        asyncio.CancelledError: If ``shutdown_event`` fires while waiting
            between attempts.
        Exception: Any non-retryable error raised by ``coro_fn``.
    """
    current_delay = delay
    while True:
        try:
            return await coro_fn()
        except AuthenticationError:
            raise
        except DBAPIError as e:
            if not e.connection_invalidated:
                raise
            logger.warning(event, delay=current_delay, reason=e.__class__.__name__)
        except (RedisConnectionError, RedisTimeoutError) as e:
            logger.warning(event, delay=current_delay, reason=e.__class__.__name__)
        if shutdown_event is None:
            await asyncio.sleep(current_delay)
        else:
            await race_wait_task(
                asyncio.create_task(asyncio.sleep(current_delay)), shutdown_event
            )
        current_delay = min(current_delay * 2, MAX_BACKOFF_DELAY)


async def race_wait_task[T](task: asyncio.Task[T], shutdown_event: asyncio.Event) -> T:
    """Await ``task`` unless ``shutdown_event`` fires first.

    Raises:
        asyncio.CancelledError: If ``shutdown_event`` fires first; ``task``
            has been told to cancel by then.
        Exception: Whatever ``task`` raised, if it finished with an error.
    """
    wait_task: asyncio.Task[bool] = asyncio.create_task(shutdown_event.wait())
    done, pending = await asyncio.wait(
        {task, wait_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for t in pending:
        t.cancel()
    await asyncio.sleep(0)
    if task in done:
        return task.result()
    raise asyncio.CancelledError
