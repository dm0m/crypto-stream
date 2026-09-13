import asyncio
from collections.abc import Iterator, Sequence
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets
from redis.exceptions import AuthenticationError as RedisAuthenticationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from sqlalchemy.exc import DBAPIError

from core.backoff import MAX_BACKOFF_DELAY, backoff, race_wait_task, retry_call
from core.log_events import LogEvent


async def set_shutdown_event(shutdown_event: asyncio.Event) -> None:
    """Set the event after a short delay, so the raced task is mid-await."""
    await asyncio.sleep(0.01)
    shutdown_event.set()


@pytest.mark.asyncio
async def test_race_wait_task_returns_result_when_task_wins() -> None:
    """task completes before shutdown fires -> returns task.result()."""
    event = asyncio.Event()
    sleep_task = asyncio.create_task(asyncio.sleep(0, result=6))
    assert await race_wait_task(sleep_task, event) == 6


@pytest.mark.asyncio
async def test_race_wait_task_raises_cancelled_error_when_shutdown_fires(
    shutdown_event: asyncio.Event,
) -> None:
    """shutdown_event set while task still pending -> raises asyncio.CancelledError."""
    setter = asyncio.create_task(set_shutdown_event(shutdown_event))
    with pytest.raises(asyncio.CancelledError):
        await race_wait_task(asyncio.create_task(asyncio.sleep(60)), shutdown_event)
    await setter


@pytest.mark.asyncio
async def test_race_wait_task_cancels_the_losing_task(
    shutdown_event: asyncio.Event,
) -> None:
    """When shutdown wins, the raced task itself ends up cancelled, not left running."""
    losing_task = asyncio.create_task(asyncio.sleep(60))
    setter = asyncio.create_task(set_shutdown_event(shutdown_event))
    with pytest.raises(asyncio.CancelledError):
        await race_wait_task(losing_task, shutdown_event)
    await setter
    assert losing_task.cancelled()


@pytest.mark.asyncio
async def test_race_wait_task_returns_immediately_when_shutdown_already_set(
    shutdown_event: asyncio.Event,
) -> None:
    """shutdown_event is already set *before* the race starts -> no hang, raises promptly."""
    shutdown_event.set()
    with pytest.raises(asyncio.CancelledError):
        await race_wait_task(asyncio.create_task(asyncio.sleep(60)), shutdown_event)


# --- shared plumbing for retry_call / backoff -----------------------------
#
# Both functions wait with ``asyncio.sleep`` and both are wrapped around
# ``race_wait_task``. Tests below never wait for real: they either patch sleep
# (fast_sleep) or, for the shutdown-during-wait cases, keep the real sleep with
# a long delay so the race, not the timer, is what ends the wait.

EVENT = LogEvent.REDIS_RECONNECTING


@pytest.fixture
def fast_sleep() -> Iterator[AsyncMock]:
    """Replace ``core.backoff.asyncio.sleep`` with a recorder that never waits."""
    real_sleep = asyncio.sleep

    async def _yield_once(delay: float, *args: object, **kwargs: object) -> None:
        await real_sleep(0)

    with patch(
        "core.backoff.asyncio.sleep", new=AsyncMock(side_effect=_yield_once)
    ) as mock:
        yield mock


@pytest.fixture
def fake_logger() -> Iterator[MagicMock]:
    with patch("core.backoff.logger") as logger:
        yield logger


def recorded_delays(fast_sleep: AsyncMock) -> list[float]:
    return [c.args[0] for c in fast_sleep.await_args_list if c.args[0] != 0]


class _ScriptedFunc:
    """Zero-argument coroutine function for ``backoff`` that plays back a script."""

    def __init__(
        self, outcomes: Sequence[BaseException | None], shutdown_event: asyncio.Event
    ) -> None:
        self._outcomes = list(outcomes)
        self._shutdown_event = shutdown_event
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1
        if not self._outcomes:
            self._shutdown_event.set()
            return
        outcome = self._outcomes.pop(0)
        if outcome is not None:
            raise outcome


# --- retry_call ------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_call_returns_result_without_retrying_on_first_success(
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """coro_fn succeeds on the first call -> its value is returned unchanged."""
    coro_fn = AsyncMock(return_value=42)
    assert await retry_call(coro_fn, event=EVENT) == 42
    coro_fn.assert_awaited_once()
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(
            RedisConnectionError("connection refused"), id="redis_connection_error"
        ),
        pytest.param(RedisTimeoutError("timed out"), id="redis_timeout_error"),
        pytest.param(
            DBAPIError(
                "stmt",
                {},
                Exception("server closed the connection"),
                connection_invalidated=True,
            ),
            id="dbapi_error_connection_invalidated",
        ),
    ],
)
async def test_retry_call_retries_transient_errors_then_returns_result(
    exc: Exception,
    fast_sleep: AsyncMock,
) -> None:
    """coro_fn raises a retryable error twice, then returns 42 -> 42."""
    coro_fn = AsyncMock(side_effect=[exc, exc, 42])
    assert await retry_call(coro_fn, event=EVENT) == 42
    assert coro_fn.await_count == 3
    assert recorded_delays(fast_sleep) == [2, 4]


@pytest.mark.asyncio
async def test_retry_call_reraises_dbapi_error_when_connection_not_invalidated(
    fast_sleep: AsyncMock,
) -> None:
    """DBAPIError with connection_invalidated=False -> re-raised at once, not retried."""
    exc = DBAPIError("stmt", {}, Exception("duplicate key value"))
    coro_fn = AsyncMock(side_effect=exc)
    with pytest.raises(DBAPIError):
        await retry_call(coro_fn, event=EVENT)
    coro_fn.assert_awaited_once()
    fast_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_call_reraises_unrelated_exceptions_immediately(
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """ValueError from coro_fn -> propagates unchanged on the first attempt."""
    coro_fn = AsyncMock(side_effect=ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        await retry_call(coro_fn, event=EVENT)
    coro_fn.assert_awaited_once()
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_retry_call_doubles_delay_and_caps_at_max_backoff_delay(
    fast_sleep: AsyncMock,
) -> None:
    """delay=40, four failures then success -> sleeps 40, 60, 60, 60."""
    coro_fn = AsyncMock(side_effect=[RedisConnectionError("down")] * 4 + [42])
    assert await retry_call(coro_fn, event=EVENT, delay=40) == 42
    assert recorded_delays(fast_sleep) == [
        40,
        MAX_BACKOFF_DELAY,
        MAX_BACKOFF_DELAY,
        MAX_BACKOFF_DELAY,
    ]


@pytest.mark.asyncio
async def test_retry_call_logs_warning_with_event_delay_and_reason(
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """Each retry logs ``event`` with the delay about to be waited and the reason."""
    coro_fn = AsyncMock(side_effect=[RedisConnectionError("down"), 42])
    await retry_call(coro_fn, event=EVENT)
    fake_logger.warning.assert_called_once_with(
        EVENT, delay=2, reason="ConnectionError"
    )


@pytest.mark.asyncio
async def test_retry_call_with_unset_shutdown_event_retries_normally(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
) -> None:
    """shutdown_event provided but never set -> same behavior as passing None."""
    coro_fn = AsyncMock(side_effect=[RedisConnectionError("down"), 42])
    assert await retry_call(coro_fn, event=EVENT, shutdown_event=shutdown_event) == 42
    assert coro_fn.await_count == 2
    assert recorded_delays(fast_sleep) == [2]


@pytest.mark.asyncio
async def test_retry_call_raises_cancelled_error_when_shutdown_fires_during_wait(
    shutdown_event: asyncio.Event,
) -> None:
    """coro_fn keeps failing; shutdown set mid-sleep -> asyncio.CancelledError."""
    coro_fn = AsyncMock(side_effect=RedisConnectionError("down"))
    setter = asyncio.create_task(set_shutdown_event(shutdown_event))
    with pytest.raises(asyncio.CancelledError):
        await retry_call(coro_fn, event=EVENT, delay=60, shutdown_event=shutdown_event)
    await setter
    coro_fn.assert_awaited_once()


# --- backoff ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_backoff_returns_immediately_when_shutdown_already_set(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
) -> None:
    """shutdown_event set before the call -> returns without ever calling func."""
    func = AsyncMock()
    shutdown_event.set()
    await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    func.assert_not_awaited()
    fast_sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_backoff_restarts_func_immediately_after_normal_return(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """func returns cleanly (a socket closed with code 1000) -> restarted with no wait."""
    func = _ScriptedFunc([None, None], shutdown_event)
    await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    assert func.calls == 3
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(
            websockets.ConnectionClosedError(None, None), id="connection_closed"
        ),
        pytest.param(OSError("connection reset"), id="os_error"),
        pytest.param(TimeoutError(), id="timeout"),
    ],
)
async def test_backoff_sleeps_then_retries_after_rapid_network_failure(
    exc: Exception,
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """func raises a retryable error once, then sets shutdown -> one wait of ``delay``."""
    func = _ScriptedFunc([exc], shutdown_event)
    await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    assert func.calls == 2
    assert recorded_delays(fast_sleep) == [2]
    fake_logger.warning.assert_called_once()
    assert fake_logger.warning.call_args.kwargs["reason"] == type(exc).__name__


@pytest.mark.asyncio
async def test_backoff_doubles_delay_and_caps_on_consecutive_rapid_failures(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """delay=20, four rapid failures then shutdown -> sleeps 20, 40, 60, 60."""
    func = _ScriptedFunc([OSError("reset")] * 4, shutdown_event)
    await backoff(func, event=EVENT, delay=20, shutdown_event=shutdown_event)
    assert recorded_delays(fast_sleep) == [20, 40, MAX_BACKOFF_DELAY, MAX_BACKOFF_DELAY]
    attempts = [c.kwargs["attempt"] for c in fake_logger.warning.call_args_list]
    assert attempts == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_backoff_resets_delay_after_healthy_run_that_then_fails(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """A failure after a long-lived run does not wait and resets the backoff."""
    func = _ScriptedFunc(
        [OSError("a"), OSError("b"), OSError("c"), OSError("d")], shutdown_event
    )
    readings = [
        0,
        1,  # run 1: rapid
        10,
        11,  # run 2: rapid
        20,
        100,  # run 3: healthy (80 s >= 60 s)
        200,
        201,  # run 4: rapid
        300,  # run 5: returns normally, only the start is read
    ]
    with patch("core.backoff.time.perf_counter", side_effect=readings):
        await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    assert recorded_delays(fast_sleep) == [2, 4, 2]
    attempts = [c.kwargs["attempt"] for c in fake_logger.warning.call_args_list]
    assert attempts == [0, 1, 0]


@pytest.mark.asyncio
async def test_backoff_resets_delay_after_successful_run(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
) -> None:
    """rapid fail, rapid fail, clean return, rapid fail, shutdown -> sleeps 2, 4, 2."""
    func = _ScriptedFunc(
        [OSError("a"), OSError("b"), None, OSError("c")], shutdown_event
    )
    await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    assert recorded_delays(fast_sleep) == [2, 4, 2]


@pytest.mark.asyncio
async def test_backoff_propagates_non_network_exceptions(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """ValueError from func -> raised out of backoff on the first attempt."""
    func = AsyncMock(side_effect=ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    func.assert_awaited_once()
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()


@pytest.mark.asyncio
async def test_backoff_returns_when_shutdown_fires_during_sleep(
    shutdown_event: asyncio.Event,
) -> None:
    """func fails fast; shutdown set mid-sleep -> backoff returns, it does not raise."""
    func = AsyncMock(side_effect=OSError("down"))
    setter = asyncio.create_task(set_shutdown_event(shutdown_event))
    await backoff(func, event=EVENT, delay=60, shutdown_event=shutdown_event)
    await setter
    func.assert_awaited_once()


@pytest.mark.asyncio
async def test_backoff_returns_without_sleeping_when_failure_arrives_after_shutdown(
    shutdown_event: asyncio.Event,
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """func sets shutdown and then raises OSError -> immediate return."""

    async def func() -> None:
        shutdown_event.set()
        raise OSError("reset")

    await backoff(func, event=EVENT, shutdown_event=shutdown_event)
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()


# --- gaps found in the 2026-09-13 coverage review ---------------------------


@pytest.mark.asyncio
async def test_race_wait_task_prefers_task_when_both_ready(
    shutdown_event: asyncio.Event,
) -> None:
    """Task result available and shutdown already set -> the result is returned."""
    shutdown_event.set()
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(7)
    assert await race_wait_task(asyncio.create_task(queue.get()), shutdown_event) == 7


@pytest.mark.asyncio
async def test_race_wait_task_propagates_task_exception(
    shutdown_event: asyncio.Event,
) -> None:
    """The raced task raises -> that exception comes out of race_wait_task."""

    async def boom() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        await race_wait_task(asyncio.create_task(boom()), shutdown_event)


@pytest.mark.asyncio
async def test_backoff_without_shutdown_event_uses_plain_sleep(
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """shutdown_event=None -> waits with a bare asyncio.sleep between failures."""
    func = AsyncMock(side_effect=[OSError("a"), OSError("b"), ValueError("stop")])
    with pytest.raises(ValueError, match="stop"):
        await backoff(func, event=EVENT)
    assert recorded_delays(fast_sleep) == [2, 4]
    assert func.await_count == 3
    assert fake_logger.warning.call_count == 2


@pytest.mark.asyncio
async def test_retry_call_reraises_authentication_error_immediately(
    fast_sleep: AsyncMock,
    fake_logger: MagicMock,
) -> None:
    """A rejected password -> re-raised on the first attempt, never retried."""
    coro_fn = AsyncMock(side_effect=RedisAuthenticationError("invalid password"))
    with pytest.raises(RedisAuthenticationError):
        await retry_call(coro_fn, event=EVENT)
    coro_fn.assert_awaited_once()
    fast_sleep.assert_not_awaited()
    fake_logger.warning.assert_not_called()
