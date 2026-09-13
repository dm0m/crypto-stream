import asyncio
import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets
from redis.asyncio import Redis

from core.log_events import LogEvent
from domain.trade import Trade
from ingestion.normalizers.binance import BinanceNormalizer
from ingestion.service import IngestionService
from processing.worker import TradeWorker
from tests.helpers import make_trade, make_trades, set_shutdown_event_after_delay


class _FakeWebSocket:
    """Minimal async-iterable stand-in for a websockets connection."""

    def __init__(
        self, messages: list[str] | None = None, hang_when_exhausted: bool = False
    ) -> None:
        self._messages = iter(messages or [])
        self._hang_when_exhausted = hang_when_exhausted
        self.sent: list[str] = []

    async def send(self, message: str) -> None:
        self.sent.append(message)

    def __aiter__(self) -> "_FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._messages)
        except StopIteration:
            if self._hang_when_exhausted:
                await asyncio.sleep(3600)
            raise StopAsyncIteration from None


class _FakeConnect:
    """Stand-in for ``websockets.connect(URL)``: an async context manager
    that hands back a pre-built ``_FakeWebSocket`` instead of opening a real
    connection."""

    def __init__(self, ws: _FakeWebSocket) -> None:
        self._ws = ws

    def __call__(self, *args: object, **kwargs: object) -> "_FakeConnect":
        return self

    async def __aenter__(self) -> _FakeWebSocket:
        return self._ws

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _raw_trade_message(trade_id: int = 1) -> str:
    return json.dumps(
        {
            "e": "trade",
            "E": 1749600000000,
            "s": "BTCUSDT",
            "t": trade_id,
            "p": "63468.98",
            "q": "0.00158",
            "T": 1749600000000,
            "m": False,
            "M": True,
        }
    )


# --- get_next_trade ---------------------------------------------------


@pytest.mark.asyncio
async def test_get_next_trade_returns_directly_when_no_shutdown_event(
    fake_redis: AsyncMock,
) -> None:
    """Construct the service with shutdown_event=None; queue.get() path, no race at all."""
    service = IngestionService(cast(Redis, fake_redis), shutdown_event=None)
    trade = make_trade()
    await service.queue.put(trade)
    assert await service.get_next_trade() == trade


@pytest.mark.asyncio
async def test_get_next_trade_returns_trade_already_in_queue(
    ingestion_service: IngestionService,
) -> None:
    """Queue has an item before racing -> the get-task wins, trade comes back normally."""
    trade = make_trade()
    await ingestion_service.queue.put(trade)
    assert await ingestion_service.get_next_trade() == trade


@pytest.mark.asyncio
async def test_get_next_trade_returns_none_when_shutdown_fires_on_empty_queue(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """Empty queue, shutdown fires -> None, not an exception (this is the contract write relies on)."""
    setter = asyncio.create_task(set_shutdown_event_after_delay(0.01, shutdown_event))
    assert await ingestion_service.get_next_trade() is None
    await setter


# --- connect_and_consume ------------------------------------------------


@pytest.mark.asyncio
async def test_connect_and_consume_returns_normally_when_receive_completes(
    ingestion_service: IngestionService,
) -> None:
    """receive finishes on its own (e.g. socket closed) -> no exception propagates."""
    fake_ws = _FakeWebSocket(messages=[])
    with patch("ingestion.service.websockets.connect", _FakeConnect(fake_ws)):
        await ingestion_service.connect_and_consume()


@pytest.mark.asyncio
async def test_connect_and_consume_swallows_cancelled_error_on_shutdown(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """The bug this whole session was about: shutdown mid-receive must not propagate out."""
    fake_ws = _FakeWebSocket(messages=[], hang_when_exhausted=True)
    setter = asyncio.create_task(set_shutdown_event_after_delay(0.01, shutdown_event))
    with patch("ingestion.service.websockets.connect", _FakeConnect(fake_ws)):
        await ingestion_service.connect_and_consume()
    await setter


# --- receive ------------------------------------------------------------


@pytest.mark.asyncio
async def test_receive_normalizes_and_queues_valid_messages(
    ingestion_service: IngestionService,
) -> None:
    """Feed receive a fake async-iterable of raw JSON strings; valid ones land in the queue."""
    fake_ws = _FakeWebSocket(messages=[_raw_trade_message(trade_id=1)])
    await ingestion_service.receive(cast(websockets.WebSocketClientProtocol, fake_ws))
    assert ingestion_service.queue.qsize() == 1
    assert ingestion_service.received == 1
    trade = ingestion_service.queue.get_nowait()
    assert trade.trade_id == "1"


@pytest.mark.asyncio
async def test_receive_skips_malformed_messages_without_queuing(
    ingestion_service: IngestionService,
) -> None:
    """A message BinanceNormalizer.normalize() rejects (returns None) must not be queued."""
    ack_message = json.dumps({"result": None, "id": 1})
    fake_ws = _FakeWebSocket(messages=[ack_message])
    await ingestion_service.receive(cast(websockets.WebSocketClientProtocol, fake_ws))
    assert ingestion_service.queue.qsize() == 0
    assert ingestion_service.received == 0


# --- flush_batch / write -----------------------------------------------


@pytest.mark.asyncio
async def test_flush_batch_pipelines_xadd_per_trade_then_executes(
    ingestion_service: IngestionService,
    fake_redis: AsyncMock,
) -> None:
    """One pipe.xadd() call per trade in the batch, pipe.execute() awaited once."""
    mock_pipeline = MagicMock()
    mock_pipeline.execute = AsyncMock()
    fake_redis.pipeline = MagicMock(return_value=mock_pipeline)

    trades = [make_trade(), make_trade()]
    await ingestion_service.flush_batch(trades)

    assert mock_pipeline.xadd.call_count == len(trades)
    mock_pipeline.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_write_batches_multiple_queued_trades_up_to_batch_size(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """Several trades already queued -> one flush_batch call with all of them, not one each."""
    trades = [make_trade(), make_trade(), make_trade()]
    for trade in trades:
        await ingestion_service.queue.put(trade)

    async def _fake_flush_batch(batch: list[Trade]) -> None:
        shutdown_event.set()

    ingestion_service.flush_batch = AsyncMock(side_effect=_fake_flush_batch)  # type: ignore[method-assign]
    await ingestion_service.write()

    ingestion_service.flush_batch.assert_awaited_once()
    flushed_batch = ingestion_service.flush_batch.await_args_list[0].args[0]
    assert len(flushed_batch) == 3


@pytest.mark.asyncio
async def test_write_returns_when_get_next_trade_returns_none(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """Shutdown + empty queue -> write's loop exits cleanly, no exception."""
    shutdown_event.set()
    ingestion_service.flush_batch = AsyncMock()  # type: ignore[method-assign]
    await ingestion_service.write()
    ingestion_service.flush_batch.assert_not_awaited()


# --- run / shutdown --------------------------------------------------------


@pytest.mark.asyncio
async def test_run_logs_shutdown_initiated_and_complete_in_order(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """Mirrors test_worker.py's equivalent; ordering matters for the log events."""
    shutdown_event.set()
    ingestion_service.logger = MagicMock()
    await ingestion_service.run()
    logged_events = [
        call.args[0] for call in ingestion_service.logger.info.call_args_list
    ]
    assert LogEvent.SHUTDOWN_INITIATED in logged_events
    assert LogEvent.SHUTDOWN_COMPLETE in logged_events
    assert logged_events.index(LogEvent.SHUTDOWN_INITIATED) < logged_events.index(
        LogEvent.SHUTDOWN_COMPLETE
    )


# --- gaps found in the 2026-09-13 coverage review ---------------------------


@pytest.mark.asyncio
async def test_run_drains_queue_to_redis_after_shutdown(
    ingestion_service: IngestionService,
    shutdown_event: asyncio.Event,
) -> None:
    """Shutdown fires with trades still queued -> every one is flushed before run returns."""
    trades = make_trades(5)
    for trade in trades:
        await ingestion_service.queue.put(trade)
    shutdown_event.set()

    ingestion_service.flush_batch = AsyncMock()  # type: ignore[method-assign]
    await ingestion_service.run()

    ingestion_service.flush_batch.assert_awaited_once()
    flushed = ingestion_service.flush_batch.await_args_list[0].args[0]
    assert [t.trade_id for t in flushed] == [t.trade_id for t in trades]
    assert ingestion_service.queue.empty()


@pytest.mark.asyncio
async def test_flush_batch_payload_is_parseable_by_worker(
    ingestion_service: IngestionService,
    fake_redis: AsyncMock,
) -> None:
    """Each xadd targets ``TradeWorker.STREAM`` with a ``data`` field the worker parses back to the same Trade."""
    mock_pipeline = MagicMock()
    mock_pipeline.execute = AsyncMock()
    fake_redis.pipeline = MagicMock(return_value=mock_pipeline)
    trade = make_trade()

    await ingestion_service.flush_batch([trade])

    stream, fields = mock_pipeline.xadd.call_args.args
    assert stream == TradeWorker.STREAM
    assert Trade.model_validate_json(fields["data"]) == trade


@pytest.mark.asyncio
async def test_connect_and_consume_sends_subscribe_for_each_symbol(
    fake_redis: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """One SUBSCRIBE frame listing ``<symbol>@trade`` per configured symbol."""
    service = IngestionService(
        cast(Redis, fake_redis),
        shutdown_event=shutdown_event,
        symbols=["btcusdt", "ethusdt"],
    )
    fake_ws = _FakeWebSocket(messages=[])
    with patch("ingestion.service.websockets.connect", _FakeConnect(fake_ws)):
        await service.connect_and_consume()

    assert [json.loads(m) for m in fake_ws.sent] == [
        {"method": "SUBSCRIBE", "params": ["btcusdt@trade", "ethusdt@trade"], "id": 1}
    ]


@pytest.mark.asyncio
async def test_receive_blocks_on_full_queue_until_drained(
    fake_redis: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """batch_size=2 and three messages -> receive parks on the third put; draining one lets it finish."""
    service = IngestionService(
        cast(Redis, fake_redis), shutdown_event=shutdown_event, batch_size=2
    )
    fake_ws = _FakeWebSocket(messages=[_raw_trade_message(i) for i in (1, 2, 3)])
    receive_task = asyncio.create_task(
        service.receive(cast(websockets.WebSocketClientProtocol, fake_ws))
    )
    await asyncio.sleep(0)

    assert not receive_task.done(), (
        "receive should be parked on queue.put, not finished"
    )
    assert service.queue.qsize() == 2
    assert service.received == 2

    service.queue.get_nowait()
    await receive_task
    assert service.received == 3
    assert service.queue.qsize() == 2


@pytest.mark.asyncio
async def test_report_metrics_computes_deltas_and_drop_ratio(
    ingestion_service: IngestionService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two ticks: counters move between them and each snapshot reports per-window deltas, not running totals."""
    ingestion_service.logger = MagicMock()
    monkeypatch.setattr(BinanceNormalizer, "malformed_trade_count", 5)
    tick = 0

    async def _sleep(interval: float) -> None:
        nonlocal tick
        tick += 1
        if tick == 1:
            ingestion_service.received = 10
            BinanceNormalizer.malformed_trade_count = 7
            await ingestion_service.queue.put(make_trade())
        elif tick == 2:
            pass  # nothing new this window
        else:
            raise asyncio.CancelledError

    with patch("ingestion.service.asyncio.sleep", new=AsyncMock(side_effect=_sleep)):
        with pytest.raises(asyncio.CancelledError):
            await ingestion_service._report_metrics(interval=5)

    snapshots = ingestion_service.logger.info.call_args_list
    assert [c.args[0] for c in snapshots] == [LogEvent.METRICS_SNAPSHOT] * 2
    first, second = (c.kwargs for c in snapshots)
    assert first["dropped_total"] == 7
    assert first["dropped_delta"] == 2
    assert first["dropped_per_sec"] == pytest.approx(0.4)
    assert first["received_per_sec"] == pytest.approx(2.0)
    assert first["drop_ratio"] == pytest.approx(0.2)
    assert first["queue_pct"] == pytest.approx(0.1)  # 1 of 1000
    assert second["dropped_delta"] == 0
    assert second["received_per_sec"] == 0.0
    assert second["drop_ratio"] == 0.0, (
        "no trades in the window must read 0.0, not divide by zero"
    )


@pytest.mark.asyncio
async def test_receive_skips_invalid_json_and_keeps_reading(
    ingestion_service: IngestionService,
) -> None:
    """A frame that is not JSON -> logged, skipped; the next valid trade is still queued."""
    ingestion_service.logger = MagicMock()
    fake_ws = _FakeWebSocket(
        messages=["<html>502 Bad Gateway</html>", _raw_trade_message(trade_id=7)]
    )
    await ingestion_service.receive(cast(websockets.WebSocketClientProtocol, fake_ws))

    assert ingestion_service.received == 1
    assert ingestion_service.queue.get_nowait().trade_id == "7"
    ingestion_service.logger.warning.assert_called_once()
    assert ingestion_service.logger.warning.call_args.args[0] == LogEvent.TRADE_DROPPED


@pytest.mark.asyncio
async def test_flush_batch_caps_stream_length(
    fake_redis: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """Every XADD carries ``MAXLEN ~ stream_maxlen`` so the stream is bounded while no worker consumes."""
    service = IngestionService(
        cast(Redis, fake_redis), shutdown_event=shutdown_event, stream_maxlen=250_000
    )
    mock_pipeline = MagicMock()
    mock_pipeline.execute = AsyncMock()
    fake_redis.pipeline = MagicMock(return_value=mock_pipeline)

    await service.flush_batch([make_trade(), make_trade()])

    for call in mock_pipeline.xadd.call_args_list:
        assert call.kwargs == {"maxlen": 250_000, "approximate": True}
