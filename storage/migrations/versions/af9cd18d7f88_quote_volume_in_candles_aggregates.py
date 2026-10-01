"""quote volume in candles aggregates

Revision ID: af9cd18d7f88
Revises: 5b974843f8aa
Create Date: 2026-09-24 17:56:30.902039

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "af9cd18d7f88"
down_revision: str | Sequence[str] | None = "5b974843f8aa"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The definitions this revision installs. quote_volume is sum(price * quantity),
# the numerator of VWAP. Storing the numerator rather than a per-bucket vwap is
# what makes VWAP exact over any window: the ratio cannot be rolled up, but the
# numerator and denominator can each be summed.
CANDLES_1M = """
CREATE MATERIALIZED VIEW candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts_event) AS ts_open,
    exchange,
    symbol,
    first(price, ts_event)            AS open,
    max(price)                        AS high,
    min(price)                        AS low,
    last(price, ts_event)             AS close,
    sum(quantity)                     AS volume,
    count(*)                          AS trade_count,
    sum(price * quantity)             AS quote_volume
FROM trades
GROUP BY ts_open, exchange, symbol
WITH NO DATA
"""

# Hierarchical: rolls the 1m aggregate up rather than rescanning trades, so the
# hourly numerator is a sum of minute numerators.
CANDLES_1H = """
CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts_open) AS ts_open,
    exchange,
    symbol,
    first(open, ts_open)           AS open,
    max(high)                      AS high,
    min(low)                       AS low,
    last(close, ts_open)           AS close,
    sum(volume)                    AS volume,
    sum(trade_count)               AS trade_count,
    sum(quote_volume)              AS quote_volume
FROM candles_1m
GROUP BY 1, exchange, symbol
WITH NO DATA
"""

# The definitions as of revision 5b974843f8aa, i.e. the same views without
# quote_volume. Kept so downgrade restores the previous schema rather than
# reinstalling the new one.
CANDLES_1M_PREV = """
CREATE MATERIALIZED VIEW candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts_event) AS ts_open,
    exchange,
    symbol,
    first(price, ts_event)            AS open,
    max(price)                        AS high,
    min(price)                        AS low,
    last(price, ts_event)             AS close,
    sum(quantity)                     AS volume,
    count(*)                          AS trade_count
FROM trades
GROUP BY ts_open, exchange, symbol
WITH NO DATA
"""

CANDLES_1H_PREV = """
CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts_open) AS ts_open,
    exchange,
    symbol,
    first(open, ts_open)           AS open,
    max(high)                      AS high,
    min(low)                       AS low,
    last(close, ts_open)           AS close,
    sum(volume)                    AS volume,
    sum(trade_count)               AS trade_count
FROM candles_1m
GROUP BY 1, exchange, symbol
WITH NO DATA
"""

# (view, start_offset, end_offset, schedule_interval). Dropping a continuous
# aggregate drops its refresh policy with it, so both are re-added after every
# rebuild.
POLICIES = (
    ("candles_1m", "1 hour", "1 minute", "1 minute"),
    ("candles_1h", "1 day", "1 hour", "1 hour"),
)


def _rebuild(minute_ddl: str, hour_ddl: str) -> None:
    """Drop both continuous aggregates and recreate them from the given DDL.

    A continuous aggregate is a materialized view that TimescaleDB keeps up to
    date incrementally, recomputing only the time ranges whose underlying rows
    changed rather than the whole view. TimescaleDB cannot add or remove one of
    its columns, so any change to its shape means dropping and recreating it.
    Both directions of this revision are that same rebuild, differing only in
    which definitions they install, which is why they share this helper: an
    asymmetry between them would otherwise be easy to miss in two near
    identical walls of statements.

    The order of these statements matters at every step. ``candles_1h`` is
    dropped first because
    it stacks on ``candles_1m``, and it is created and refreshed last for the
    same reason: a hierarchical aggregate rolls up whatever its parent holds
    at the time, so refreshing it before the minute view is populated would
    materialize nothing. The refreshes are explicit because ``WITH NO DATA``
    leaves both views empty and a refresh policy only ever materializes its
    own ``start_offset``, walking forward from whenever it first ran and never
    backfilling history. ``materialized_only = false`` restores real-time
    aggregation, without which the still-open current bucket disappears from
    reads.

    Timescale refuses to create a continuous aggregate inside a transaction,
    hence the ``autocommit_block``. Every statement therefore commits on its
    own and a failure midway leaves the earlier ones applied, so the drops use
    ``IF EXISTS`` to keep a second attempt from dying on the views the first
    attempt already removed.

    Args:
        minute_ddl: ``CREATE MATERIALIZED VIEW`` for ``candles_1m``.
        hour_ddl: ``CREATE MATERIALIZED VIEW`` for ``candles_1h``, which must
            select from ``candles_1m``.
    """
    with op.get_context().autocommit_block():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1h")
        op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1m")
        op.execute(minute_ddl)
        op.execute("CALL refresh_continuous_aggregate('candles_1m', NULL, NULL)")
        op.execute(hour_ddl)
        op.execute("CALL refresh_continuous_aggregate('candles_1h', NULL, NULL)")
        for view in ("candles_1m", "candles_1h"):
            op.execute(
                f"ALTER MATERIALIZED VIEW {view} SET (timescaledb.materialized_only = false)"
            )
        for view, start, end, every in POLICIES:
            op.execute(
                f"SELECT add_continuous_aggregate_policy('{view}', "
                f"start_offset => INTERVAL '{start}', "
                f"end_offset => INTERVAL '{end}', "
                f"schedule_interval => INTERVAL '{every}')"
            )


def upgrade() -> None:
    """Rebuild both aggregates with a ``quote_volume`` column.

    ``quote_volume`` is ``sum(price * quantity)`` per bucket: the numerator of
    the volume-weighted average price (VWAP), which is total money traded
    divided by total quantity traded. Nothing stored previously held it, so
    VWAP could only be obtained by scanning every raw trade in the window.
    Reading it from a bucket turns that scan into a sum over a few hundred
    rows.

    The column holds the numerator rather than a finished per-bucket VWAP on
    purpose. An average cannot be averaged: combining buckets means summing
    their numerators and their denominators separately, so a stored ratio
    could not be rolled up into a wider window, while a stored numerator can.

    Rebuilding a continuous aggregate destroys no data as long as the table it
    derives from still covers the same period, since the rebuild recomputes
    every bucket from that table. Here the source is ``trades``, which this
    revision does not touch. The case to watch for is a deployment whose
    retention policy has already deleted trades older than the aggregate's
    oldest bucket: those buckets cannot be recomputed and would be lost.
    """
    _rebuild(CANDLES_1M, CANDLES_1H)


def downgrade() -> None:
    """Rebuild both aggregates without ``quote_volume``."""
    _rebuild(CANDLES_1M_PREV, CANDLES_1H_PREV)
