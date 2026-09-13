import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from redis import ResponseError

from core.log_events import LogEvent
from processing.worker import StreamResp, TradeWorker
from tests.helpers import make_entry, set_shutdown_event_after_delay

# --- ensure_group -----------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_group_creates_group_when_missing(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xgroup_create.return_value = True
    await worker.ensure_group()
    fake_redis.xgroup_create.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, mkstream=True
    )


@pytest.mark.asyncio
async def test_ensure_group_swallows_busygroup_error(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xgroup_create.side_effect = ResponseError(
        "BUSYGROUP Consumer Group name already exists"
    )
    await worker.ensure_group()  # must not raise


@pytest.mark.asyncio
async def test_ensure_group_reraises_non_busygroup_response_error(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xgroup_create.side_effect = ResponseError(
        "WRONGTYPE Operation against a key"
    )
    with pytest.raises(ResponseError):
        await worker.ensure_group()


# --- consume / process_batch / _flush / _ack -------------------------


@pytest.mark.asyncio
async def test_consume_reads_batch_and_bulk_inserts_trades(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    entry_id, fields = make_entry()
    resp: StreamResp = [(TradeWorker.STREAM, [(entry_id, fields)])]

    def _once(*args: object, **kwargs: object) -> StreamResp:
        shutdown_event.set()
        return resp

    fake_redis.xreadgroup.side_effect = _once
    await worker.consume()
    fake_redis.xreadgroup.assert_awaited_once()
    fake_trade_repository.bulk_insert.assert_awaited_once()
    inserted_trades = fake_trade_repository.bulk_insert.await_args.args[0]
    assert len(inserted_trades) == 1
    assert inserted_trades[0].trade_id == "123456789"
    fake_redis.xack.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, entry_id
    )


@pytest.mark.asyncio
async def test_consume_acks_only_after_successful_insert(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """Ordering: bulk_insert must happen before xack, not the reverse."""
    entry_id, fields = make_entry()
    resp: StreamResp = [(TradeWorker.STREAM, [(entry_id, fields)])]
    call_order: list[str] = []

    def _once(*args: object, **kwargs: object) -> StreamResp:
        shutdown_event.set()
        return resp

    async def _record_insert(*args: object, **kwargs: object) -> None:
        call_order.append("insert")

    async def _record_ack(*args: object, **kwargs: object) -> None:
        call_order.append("ack")

    fake_redis.xreadgroup.side_effect = _once
    fake_trade_repository.bulk_insert.side_effect = _record_insert
    fake_redis.xack.side_effect = _record_ack

    await worker.consume()
    assert call_order == ["insert", "ack"]


@pytest.mark.asyncio
async def test_consume_skips_processing_on_empty_response(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    def _once_empty(*args: object, **kwargs: object) -> StreamResp:
        shutdown_event.set()
        return []

    fake_redis.xreadgroup.side_effect = _once_empty
    await worker.consume()
    fake_trade_repository.bulk_insert.assert_not_awaited()
    fake_redis.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_consume_stops_when_shutdown_event_is_set(
    worker: TradeWorker, fake_redis: AsyncMock, shutdown_event: asyncio.Event
) -> None:
    shutdown_event.set()
    await worker.consume()
    fake_redis.xreadgroup.assert_not_awaited()


@pytest.mark.asyncio
async def test_consume_logs_processing_error_and_reraises_on_unexpected_exception(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xreadgroup.side_effect = RuntimeError("boom")
    worker.logger = MagicMock()

    with pytest.raises(RuntimeError, match="boom"):
        await worker.consume()

    worker.logger.exception.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch_does_not_flush_when_no_trades(
    worker: TradeWorker, fake_trade_repository: AsyncMock
) -> None:
    await worker.process_batch([(TradeWorker.STREAM, [])])
    fake_trade_repository.bulk_insert.assert_not_awaited()


# --- recover_failed_msgs -------------------------------------------------


@pytest.mark.asyncio
async def test_recover_claims_idle_pending_entries(
    worker: TradeWorker, fake_redis: AsyncMock, fake_trade_repository: AsyncMock
) -> None:
    entry_id, fields = make_entry()
    fake_redis.xautoclaim.return_value = ("0-0", [(entry_id, fields)], [])
    worker.get_delivery_count = AsyncMock(return_value=1)  # type: ignore[method-assign]
    await worker.recover_failed_msgs()
    fake_redis.xautoclaim.assert_awaited_once_with(
        TradeWorker.STREAM,
        TradeWorker.GROUP,
        worker.RECOVERY_CONSUMER,
        TradeWorker.MIN_IDLE_TIME,
        "0-0",
    )
    fake_trade_repository.bulk_insert.assert_awaited_once()


@pytest.mark.asyncio
async def test_recover_paginates_until_cursor_returns_zero(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    """Regression test: start must advance to next_id each iteration."""
    fake_redis.xautoclaim.side_effect = [
        ("17-0", [], []),
        ("0-0", [], []),
    ]

    await worker.recover_failed_msgs()
    assert fake_redis.xautoclaim.await_count == 2
    first_call_start = fake_redis.xautoclaim.await_args_list[0].args[-1]
    second_call_start = fake_redis.xautoclaim.await_args_list[1].args[-1]
    assert first_call_start == "0-0"
    assert second_call_start == "17-0"


@pytest.mark.asyncio
async def test_recover_reinserts_and_acks_normal_reclaimed_messages(
    worker: TradeWorker, fake_redis: AsyncMock, fake_trade_repository: AsyncMock
) -> None:
    entry_id, fields = make_entry("5-0")
    fake_redis.xautoclaim.return_value = ("0-0", [(entry_id, fields)], [])
    worker.get_delivery_count = AsyncMock(return_value=1)  # type: ignore[method-assign]
    await worker.recover_failed_msgs()
    inserted = fake_trade_repository.bulk_insert.await_args.args[0]
    assert len(inserted) == 1
    fake_redis.xack.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, entry_id
    )


@pytest.mark.asyncio
async def test_recover_sends_to_dlq_when_delivery_count_exceeds_max_retries(
    worker: TradeWorker, fake_redis: AsyncMock, fake_trade_repository: AsyncMock
) -> None:
    entry_id, fields = make_entry("9-0")
    fake_redis.xautoclaim.return_value = ("0-0", [(entry_id, fields)], [])
    worker.get_delivery_count = AsyncMock(return_value=TradeWorker.MAX_RETRIES)  # type: ignore[method-assign]
    await worker.recover_failed_msgs()
    fake_redis.xadd.assert_awaited_once()
    dlq_stream_arg = fake_redis.xadd.await_args.args[0]
    assert dlq_stream_arg == TradeWorker.DLQ_STREAM
    fake_trade_repository.bulk_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_recover_acks_dlq_routed_message_so_it_leaves_pel(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    """Regression test: xack must be called with GROUP, not DLQ_STREAM."""
    entry_id, fields = make_entry("9-0")
    fake_redis.xautoclaim.return_value = ("0-0", [(entry_id, fields)], [])
    worker.get_delivery_count = AsyncMock(return_value=TradeWorker.MAX_RETRIES)  # type: ignore[method-assign]
    await worker.recover_failed_msgs()
    fake_redis.xack.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, entry_id
    )


@pytest.mark.asyncio
async def test_recover_does_not_insert_dlq_routed_message_as_trade(
    worker: TradeWorker, fake_redis: AsyncMock, fake_trade_repository: AsyncMock
) -> None:
    """Regression test: the DLQ branch must `continue`, not fall through."""
    dlq_entry_id, dlq_fields = make_entry("1-0")
    ok_entry_id, ok_fields = make_entry("2-0")
    fake_redis.xautoclaim.return_value = (
        "0-0",
        [(dlq_entry_id, dlq_fields), (ok_entry_id, ok_fields)],
        [],
    )
    worker.get_delivery_count = AsyncMock(  # type: ignore[method-assign]
        side_effect=[TradeWorker.MAX_RETRIES, 1]
    )
    await worker.recover_failed_msgs()
    inserted = fake_trade_repository.bulk_insert.await_args.args[0]
    assert len(inserted) == 1
    fake_redis.xadd.assert_awaited_once()


@pytest.mark.asyncio
async def test_recover_logs_and_swallows_keyerror_or_validationerror(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    bad_entry_id = "3-0"
    fake_redis.xautoclaim.return_value = ("0-0", [(bad_entry_id, {})], [])
    worker.get_delivery_count = AsyncMock(return_value=1)  # type: ignore[method-assign]
    worker.logger = MagicMock()
    await worker.recover_failed_msgs()  # must not raise
    worker.logger.exception.assert_called_once()


# --- send_to_dlq / get_delivery_count -------------------------------------


@pytest.mark.asyncio
async def test_send_to_dlq_writes_expected_fields_to_dlq_stream(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    await worker.send_to_dlq("7-0", {"data": "payload"}, 4)
    fake_redis.xadd.assert_awaited_once()
    stream, payload = fake_redis.xadd.await_args.args
    assert stream == TradeWorker.DLQ_STREAM
    assert payload["data"] == "payload"
    assert payload["message_id"] == "7-0"
    assert payload["delivery_count"] == 4
    assert payload["error"] == "max_retries_exceeded"


@pytest.mark.asyncio
async def test_get_delivery_count_returns_times_delivered_from_xpending_range(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xpending_range.return_value = [{"times_delivered": 4}]
    result = await worker.get_delivery_count("2-0")
    assert result == 4
    fake_redis.xpending_range.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, "2-0", "2-0", 1
    )


@pytest.mark.asyncio
async def test_get_delivery_count_returns_zero_when_no_pending_entry(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    fake_redis.xpending_range.return_value = []
    result = await worker.get_delivery_count("2-0")
    assert result == 0


# --- run / shutdown --------------------------------------------------------


@pytest.mark.asyncio
async def test_run_creates_group_before_starting_background_tasks(
    worker: TradeWorker,
) -> None:
    """Ordering: ensure_group must complete before recovery/DLQ tasks touch the stream."""
    call_order: list[str] = []

    async def ensure_group() -> None:
        call_order.append("ensure_group")

    async def _recover() -> None:
        call_order.append("recover")

    async def _log_dlq(delay: int = 30) -> None:
        call_order.append("log_dlq")

    async def consume() -> None:
        await asyncio.sleep(0)  # let the just-created background tasks run
        call_order.append("consume")

    worker.ensure_group = ensure_group  # type: ignore[method-assign]
    worker.recover_failed_msgs = _recover  # type: ignore[method-assign]
    worker.log_dlq_count = _log_dlq  # type: ignore[method-assign]
    worker.consume = consume  # type: ignore[method-assign]
    await worker.run()
    assert call_order[0] == "ensure_group"
    assert "recover" in call_order
    assert "log_dlq" in call_order


@pytest.mark.asyncio
async def test_run_cancels_recovery_and_dlq_tasks_on_shutdown(
    worker: TradeWorker,
) -> None:
    cancelled: list[str] = []

    async def _hang(name: str) -> None:
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            cancelled.append(name)
            raise

    async def consume() -> None:
        await asyncio.sleep(0)  # let the background tasks start running

    worker.ensure_group = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.recover_failed_msgs = lambda: _hang("recover")  # type: ignore[method-assign]
    worker.log_dlq_count = lambda delay=30: _hang("log_dlq")  # type: ignore[method-assign]
    worker.consume = consume  # type: ignore[method-assign]
    await worker.run()
    assert set(cancelled) == {"recover", "log_dlq"}


@pytest.mark.asyncio
async def test_run_logs_shutdown_initiated_and_complete(worker: TradeWorker) -> None:
    worker.ensure_group = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.recover_failed_msgs = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.log_dlq_count = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.consume = AsyncMock(return_value=None)  # type: ignore[method-assign]
    worker.logger = MagicMock()
    await worker.run()
    logged_events = [call.args[0] for call in worker.logger.info.call_args_list]
    assert LogEvent.SHUTDOWN_INITIATED in logged_events
    assert LogEvent.SHUTDOWN_COMPLETE in logged_events
    assert logged_events.index(LogEvent.SHUTDOWN_INITIATED) < logged_events.index(
        LogEvent.SHUTDOWN_COMPLETE
    )


# --- log_dlq_count ----------------------------------------------------


@pytest.mark.asyncio
async def test_log_dlq_count_logs_when_dlq_non_empty(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    call_count = 0

    def _xlen(*args: object, **kwargs: object) -> int:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise asyncio.CancelledError()
        return 5

    fake_redis.xlen.side_effect = _xlen
    worker.logger = MagicMock()

    with patch("processing.worker.asyncio.sleep", new=AsyncMock()) as mock_sleep:
        with pytest.raises(asyncio.CancelledError):
            await worker.log_dlq_count(delay=0)

    worker.logger.info.assert_called_once_with(LogEvent.DLQ_NOT_EMPTY)
    mock_sleep.assert_awaited_once_with(0)


@pytest.mark.asyncio
async def test_log_dlq_count_does_not_log_when_dlq_empty(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    call_count = 0

    def _xlen(*args: object, **kwargs: object) -> int:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise asyncio.CancelledError()
        return 0

    fake_redis.xlen.side_effect = _xlen
    worker.logger = MagicMock()

    with patch("processing.worker.asyncio.sleep", new=AsyncMock()):
        with pytest.raises(asyncio.CancelledError):
            await worker.log_dlq_count(delay=0)

    worker.logger.info.assert_not_called()


@pytest.mark.asyncio
async def test_log_dlq_count_sleeps_between_polls_even_when_dlq_empty(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    """Regression test: the empty-count branch must still sleep, not busy-loop."""
    call_count = 0

    def _xlen(*args: object, **kwargs: object) -> int:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            raise asyncio.CancelledError()
        return 0

    fake_redis.xlen.side_effect = _xlen

    with patch("processing.worker.asyncio.sleep", new=AsyncMock()) as mock_sleep:
        with pytest.raises(asyncio.CancelledError):
            await worker.log_dlq_count(delay=7)

    mock_sleep.assert_awaited_once_with(7)


# --- gaps found in the 2026-09-13 coverage review ---------------------------


@pytest.mark.asyncio
async def test_consume_malformed_entry_aborts_batch_before_insert_or_ack(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
) -> None:
    """One unparseable entry in a batch -> ValidationError escapes consume,
    PROCESSING_ERROR is logged, and neither bulk_insert nor xack ran for any
    entry of that batch, the valid one included.
    """
    good_id, good_fields = make_entry("1-0")
    resp: StreamResp = [
        (TradeWorker.STREAM, [(good_id, good_fields), ("2-0", {"data": "not a trade"})])
    ]
    fake_redis.xreadgroup.return_value = resp
    worker.logger = MagicMock()

    with pytest.raises(ValidationError):
        await worker.consume()

    fake_trade_repository.bulk_insert.assert_not_awaited()
    fake_redis.xack.assert_not_awaited()
    worker.logger.exception.assert_called_once()
    assert worker.logger.exception.call_args.args[0] == LogEvent.PROCESSING_ERROR
    assert worker.processed_count == 0


async def _block_forever(*args: object, **kwargs: object) -> None:
    await asyncio.sleep(3600)


@pytest.mark.asyncio
async def test_consume_raises_cancelled_error_when_shutdown_fires_mid_read(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    fake_trade_repository: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """XREADGROUP is blocking when shutdown fires -> CancelledError escapes consume, nothing was processed."""
    fake_redis.xreadgroup.side_effect = _block_forever
    worker.logger = MagicMock()
    setter = asyncio.create_task(set_shutdown_event_after_delay(0.01, shutdown_event))

    with pytest.raises(asyncio.CancelledError):
        await worker.consume()
    await setter

    fake_trade_repository.bulk_insert.assert_not_awaited()
    fake_redis.xack.assert_not_awaited()
    worker.logger.exception.assert_not_called()


@pytest.mark.asyncio
async def test_run_absorbs_cancelled_error_from_consume(
    worker: TradeWorker,
    fake_redis: AsyncMock,
    shutdown_event: asyncio.Event,
) -> None:
    """Real consume loop, shutdown mid-read -> run returns normally and logs
    SHUTDOWN_INITIATED before SHUTDOWN_COMPLETE.
    """
    fake_redis.xreadgroup.side_effect = _block_forever
    fake_redis.xautoclaim.return_value = ("0-0", [], [])
    fake_redis.xinfo_consumers.return_value = []
    fake_redis.xlen.return_value = 0
    worker.logger = MagicMock()
    setter = asyncio.create_task(set_shutdown_event_after_delay(0.01, shutdown_event))

    await worker.run()
    await setter

    logged_events = [call.args[0] for call in worker.logger.info.call_args_list]
    assert logged_events.index(LogEvent.SHUTDOWN_INITIATED) < logged_events.index(
        LogEvent.SHUTDOWN_COMPLETE
    )
    worker.logger.exception.assert_not_called()


# --- prune_dead_consumers -------------------------------------------------


@pytest.mark.asyncio
async def test_prune_dead_consumers_deletes_only_idle_consumers_with_nothing_pending(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    """Four consumers, one deletable: idle past MIN_IDLE_TIME with pending 0."""
    dead_idle = TradeWorker.MIN_IDLE_TIME
    fake_redis.xinfo_consumers.return_value = [
        {"name": worker.CONSUMER, "pending": 0, "idle": dead_idle},
        {"name": "worker_1", "pending": 0, "idle": 500},
        {"name": "worker_2", "pending": 3, "idle": dead_idle},
        {"name": "worker_3", "pending": 0, "idle": dead_idle},
    ]

    await worker.prune_dead_consumers()

    fake_redis.xgroup_delconsumer.assert_awaited_once_with(
        TradeWorker.STREAM, TradeWorker.GROUP, "worker_3"
    )


@pytest.mark.asyncio
async def test_recover_prunes_consumers_after_the_sweep(
    worker: TradeWorker, fake_redis: AsyncMock
) -> None:
    """Pruning runs once the pending entries have been reclaimed, and even when the sweep aborted on a bad entry."""
    fake_redis.xautoclaim.return_value = ("0-0", [("3-0", {})], [])
    worker.get_delivery_count = AsyncMock(return_value=1)  # type: ignore[method-assign]
    worker.logger = MagicMock()
    worker.prune_dead_consumers = AsyncMock()  # type: ignore[method-assign]

    await worker.recover_failed_msgs()

    worker.logger.exception.assert_called_once()
    worker.prune_dead_consumers.assert_awaited_once()
