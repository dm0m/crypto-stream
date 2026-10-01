"""Unit tests for the ``/health`` readiness check."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from structlog.testing import capture_logs

from api.routers import health
from core.log_events import LogEvent

Check = Callable[[], Awaitable[None]]


async def ok() -> None:
    """A dependency that answers."""


async def refused() -> None:
    raise ConnectionRefusedError("connection refused")


async def hangs() -> None:
    await asyncio.sleep(10)


def sleeps(seconds: float) -> Check:
    """A dependency that answers after ``seconds``."""

    async def check() -> None:
        await asyncio.sleep(seconds)

    return check


def use_checks(monkeypatch: pytest.MonkeyPatch, **checks: Check) -> None:
    for name, check in checks.items():
        monkeypatch.setitem(health.CHECKS, name, check)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client for an app serving only the health router."""
    app = FastAPI()
    app.include_router(health.router)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


# --- probe ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_reports_ok_and_logs_nothing() -> None:
    """A healthy dependency produces no log noise on every probe."""
    with capture_logs() as logs:
        assert await health.probe("redis", ok) == "ok"
    assert logs == []


@pytest.mark.asyncio
async def test_probe_reports_failure_without_raising() -> None:
    """A failed check becomes an answer, logged with the dependency's name."""
    with capture_logs() as logs:
        assert await health.probe("redis", refused) == "unavailable"
    assert len(logs) == 1
    assert logs[0]["event"] == LogEvent.HEALTH_CHECK_FAILED
    assert logs[0]["dependency"] == "redis"
    assert logs[0]["error"] == "ConnectionRefusedError"
    assert logs[0]["log_level"] == "warning"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [OSError, ValueError, RuntimeError, KeyError])
async def test_probe_catches_any_exception_type(error: type[Exception]) -> None:
    """Failures do not all arrive as driver errors; none may escape as a 500."""

    async def fails() -> None:
        raise error("boom")

    assert await health.probe("postgres", fails) == "unavailable"


@pytest.mark.asyncio
async def test_probe_gives_up_on_a_hanging_dependency() -> None:
    """A server that never answers must not hold the probe open."""
    start = time.perf_counter()
    assert await health.probe("postgres", hangs) == "unavailable"
    assert time.perf_counter() - start < 2


# --- the endpoint -------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_dependencies_up_is_200(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ready: every dependency answered."""
    use_checks(monkeypatch, redis=ok, postgres=ok)
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"redis": "ok", "postgres": "ok"}


@pytest.mark.asyncio
async def test_one_dependency_down_is_503_and_the_other_is_still_checked(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Redis failure must not hide whether Postgres is fine."""
    use_checks(monkeypatch, redis=refused, postgres=ok)
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json() == {"redis": "unavailable", "postgres": "ok"}


@pytest.mark.asyncio
async def test_all_dependencies_down_is_503(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every failure is reported, not only the first one found."""
    use_checks(monkeypatch, redis=refused, postgres=refused)
    response = await client.get("/health")
    assert response.status_code == 503
    assert response.json() == {"redis": "unavailable", "postgres": "unavailable"}


@pytest.mark.asyncio
async def test_a_hanging_dependency_is_503_not_a_hang(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The endpoint as a whole answers within about one probe timeout."""
    use_checks(monkeypatch, redis=ok, postgres=hangs)
    start = time.perf_counter()
    response = await client.get("/health")
    assert response.status_code == 503
    assert time.perf_counter() - start < 2


@pytest.mark.asyncio
async def test_checks_run_concurrently(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two 0.3s checks take about 0.3s together, not 0.6s one after the other."""
    use_checks(monkeypatch, redis=sleeps(0.3), postgres=sleeps(0.3))
    start = time.perf_counter()
    response = await client.get("/health")
    assert response.status_code == 200
    assert time.perf_counter() - start < 0.55
