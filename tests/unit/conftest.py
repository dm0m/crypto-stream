import asyncio
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from redis.asyncio import Redis

from domain.enums import Interval
from ingestion.service import IngestionService
from processing.aggregator import CandleAggregator
from processing.worker import TradeWorker
from storage.repositories import CandleRepository, TradeRepository


@pytest.fixture
def fake_redis() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def fake_trade_repository() -> AsyncMock:
    return AsyncMock(spec=TradeRepository)


@pytest.fixture
def shutdown_event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture
def worker(
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> TradeWorker:
    return TradeWorker(
        consumer="worker-1-test",
        recovery_consumer="recovery-1-test",
        redis_client=cast(Redis, fake_redis),
        trade_repository=cast(TradeRepository, fake_trade_repository),
        shutdown_event=shutdown_event,
    )


@pytest.fixture
def ingestion_service(
    fake_redis: AsyncMock,
    shutdown_event: asyncio.Event,
) -> IngestionService:
    return IngestionService(
        cast(Redis, fake_redis),
        shutdown_event=shutdown_event,
    )


@pytest.fixture
def fake_candle_repository() -> AsyncMock:
    return AsyncMock(spec=CandleRepository)


@pytest.fixture
def aggregating_worker(
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    fake_candle_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> TradeWorker:
    return TradeWorker(
        consumer="worker-1-test",
        recovery_consumer="recovery-1-test",
        redis_client=cast(Redis, fake_redis),
        trade_repository=cast(TradeRepository, fake_trade_repository),
        shutdown_event=shutdown_event,
        aggregator=CandleAggregator(Interval.M1, timedelta(seconds=2)),
        candle_repository=cast(CandleRepository, fake_candle_repository),
    )
