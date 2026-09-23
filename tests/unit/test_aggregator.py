"""Unit tests for ``CandleAggregator``: pure, no I/O, plain trade lists."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from domain.candle import Candle
from domain.enums import Exchange, Interval, Side
from domain.trade import Trade
from processing.aggregator import CandleAggregator

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
GRACE = timedelta(seconds=2)


def trade_at(
    ts: datetime,
    price: str = "100",
    qty: str = "1",
    symbol: str = "BTC-USDT",
    trade_id: str | None = None,
) -> Trade:
    return Trade(
        trade_id=trade_id or str(int(ts.timestamp() * 1_000_000)),
        exchange=Exchange.BINANCE,
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(qty),
        side=Side.BUY,
        ts_event=ts,
        ts_ingest=ts,
    )


@pytest.fixture
def agg() -> CandleAggregator:
    return CandleAggregator(Interval.M1, GRACE)


# ---------------------------------------------------------------------------
# add: opening and updating a bucket
# ---------------------------------------------------------------------------


def test_first_trade_opens_bucket_and_closes_nothing(agg: CandleAggregator) -> None:
    assert agg.add(trade_at(T0)) is None
    assert agg.open_series == 1


def test_same_bucket_trade_updates_running_values(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0, price="100", qty="1"))
    agg.add(trade_at(T0 + timedelta(seconds=10), price="120", qty="2"))
    agg.add(trade_at(T0 + timedelta(seconds=20), price="90", qty="3"))
    agg.add(trade_at(T0 + timedelta(seconds=59), price="105", qty="4"))

    # Force the close by moving to the next bucket and inspect the result.
    closed = agg.add(trade_at(T0 + timedelta(minutes=1)))
    assert closed is not None
    assert closed.open == Decimal("100")
    assert closed.high == Decimal("120")
    assert closed.low == Decimal("90")
    assert closed.close == Decimal("105")
    assert closed.volume == Decimal("10")
    assert closed.trade_count == 4
    assert closed.ts_open == T0
    assert closed.interval is Interval.M1


def test_last_microsecond_of_minute_stays_in_bucket(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0))
    assert agg.add(trade_at(T0 + timedelta(seconds=59, microseconds=999_999))) is None


# ---------------------------------------------------------------------------
# add: rolling over to a new bucket
# ---------------------------------------------------------------------------


def test_next_bucket_trade_closes_previous_and_opens_new(
    agg: CandleAggregator,
) -> None:
    agg.add(trade_at(T0, price="100"))
    closed = agg.add(trade_at(T0 + timedelta(minutes=1), price="200", qty="5"))

    assert isinstance(closed, Candle)
    assert closed.ts_open == T0
    assert closed.close == Decimal("100")
    # The new bucket starts from the rolling trade only.
    nxt = agg.add(trade_at(T0 + timedelta(minutes=2)))
    assert nxt is not None
    assert nxt.ts_open == T0 + timedelta(minutes=1)
    assert nxt.open == nxt.close == Decimal("200")
    assert nxt.trade_count == 1


def test_gap_of_several_minutes_emits_one_candle_only(agg: CandleAggregator) -> None:
    """No synthetic empty candles for the quiet minutes in between."""
    agg.add(trade_at(T0))
    closed = agg.add(trade_at(T0 + timedelta(minutes=5)))
    assert closed is not None
    assert closed.ts_open == T0
    assert agg.open_series == 1


def test_series_are_independent(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0, symbol="BTC-USDT", price="100"))
    agg.add(trade_at(T0, symbol="ETH-USDT", price="10"))
    assert agg.open_series == 2

    closed = agg.add(trade_at(T0 + timedelta(minutes=1), symbol="BTC-USDT"))
    assert closed is not None
    assert closed.symbol == "BTC-USDT"
    assert closed.close == Decimal("100")
    # ETH is still open and untouched.
    assert agg.open_series == 2


# ---------------------------------------------------------------------------
# add: late trades
# ---------------------------------------------------------------------------


def test_late_trade_is_dropped_and_counted(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0 + timedelta(minutes=1), price="200"))
    assert agg.add(trade_at(T0, price="1")) is None
    assert agg.late_trades == 1

    closed = agg.add(trade_at(T0 + timedelta(minutes=2)))
    assert closed is not None
    assert closed.low == Decimal("200"), "late trade must not leak into the open bucket"


def test_trade_for_flushed_bucket_is_late(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0))
    agg.flush_expired(T0 + timedelta(minutes=1) + GRACE)
    # The bucket is gone; a trade for it starts a new bucket rather than being
    # late, because nothing newer is open for that series yet.
    assert agg.add(trade_at(T0 + timedelta(seconds=30))) is None
    assert agg.late_trades == 0
    assert agg.open_series == 1


# ---------------------------------------------------------------------------
# flush_expired
# ---------------------------------------------------------------------------


def test_flush_before_deadline_closes_nothing(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0))
    just_before = T0 + timedelta(minutes=1) + GRACE - timedelta(microseconds=1)
    assert agg.flush_expired(just_before) == []
    assert agg.open_series == 1


def test_flush_at_deadline_closes_and_forgets(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0, price="100", qty="2"))
    closed = agg.flush_expired(T0 + timedelta(minutes=1) + GRACE)
    assert len(closed) == 1
    assert closed[0].ts_open == T0
    assert closed[0].volume == Decimal("2")
    assert agg.open_series == 0


def test_flush_only_closes_expired_series(agg: CandleAggregator) -> None:
    agg.add(trade_at(T0, symbol="BTC-USDT"))
    agg.add(trade_at(T0 + timedelta(minutes=1), symbol="ETH-USDT"))
    closed = agg.flush_expired(T0 + timedelta(minutes=1) + GRACE)
    assert [c.symbol for c in closed] == ["BTC-USDT"]
    assert agg.open_series == 1


def test_flush_with_nothing_open_returns_empty(agg: CandleAggregator) -> None:
    assert agg.flush_expired(T0) == []


def test_hourly_interval_buckets_by_hour() -> None:
    agg = CandleAggregator(Interval.H1, GRACE)
    agg.add(trade_at(T0 + timedelta(minutes=5)))
    assert agg.add(trade_at(T0 + timedelta(minutes=59))) is None
    closed = agg.add(trade_at(T0 + timedelta(hours=1)))
    assert closed is not None
    assert closed.ts_open == T0
    assert closed.interval is Interval.H1


# ---------------------------------------------------------------------------
# property: any in-bucket trade list yields a consistent candle
# ---------------------------------------------------------------------------

prices = st.decimals(
    min_value=Decimal("0.00000001"), max_value=Decimal("1000000"), places=8
)
quantities = st.decimals(
    min_value=Decimal("0.00000001"), max_value=Decimal("1000"), places=8
)
offsets_us = st.integers(min_value=0, max_value=59_999_999)


@settings(max_examples=200)
@given(st.lists(st.tuples(offsets_us, prices, quantities), min_size=1, max_size=50))
def test_candle_invariants_hold_for_any_trade_sequence(
    rows: list[tuple[int, Decimal, Decimal]],
) -> None:
    agg = CandleAggregator(Interval.M1, GRACE)
    # Sort by time: the aggregator assumes in-order delivery within a bucket.
    rows.sort(key=lambda r: r[0])
    for i, (us, price, qty) in enumerate(rows):
        ts = T0 + timedelta(microseconds=us)
        assert agg.add(trade_at(ts, str(price), str(qty), trade_id=str(i))) is None

    closed = agg.flush_expired(T0 + timedelta(minutes=1) + GRACE)
    assert len(closed) == 1
    c = closed[0]
    assert c.open == rows[0][1]
    assert c.close == rows[-1][1]
    assert c.high == max(r[1] for r in rows)
    assert c.low == min(r[1] for r in rows)
    assert c.volume == sum((r[2] for r in rows), Decimal(0))
    assert c.trade_count == len(rows)
