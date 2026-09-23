"""continuous aggregates candles_1m and candles_1h

Revision ID: f75fde4ffc71
Revises: fd60264121ba
Create Date: 2026-09-16 21:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f75fde4ffc71"
down_revision: str | Sequence[str] | None = "fd60264121ba"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CANDLES_1M = """
CREATE MATERIALIZED VIEW candles_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', ts_event) AS bucket,
    exchange,
    symbol,
    first(price, ts_event)            AS open,
    max(price)                        AS high,
    min(price)                        AS low,
    last(price, ts_event)             AS close,
    sum(quantity)                     AS volume,
    count(*)                          AS trade_count
FROM trades
GROUP BY bucket, exchange, symbol
WITH NO DATA
"""

# Hierarchical: rolls the 1m aggregate up rather than rescanning trades.
CANDLES_1H = """
CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    exchange,
    symbol,
    first(open, bucket)           AS open,
    max(high)                     AS high,
    min(low)                      AS low,
    last(close, bucket)           AS close,
    sum(volume)                   AS volume,
    sum(trade_count)              AS trade_count
FROM candles_1m
GROUP BY 1, exchange, symbol
WITH NO DATA
"""

# (view, start_offset, end_offset, schedule_interval). end_offset keeps the
# still-changing current bucket out of the materialized range; real-time
# aggregation fills it on read. start_offset must stay shorter than the
# retention window on trades or refreshes cannot recompute dropped ranges.
POLICIES = (
    ("candles_1m", "1 hour", "1 minute", "1 minute"),
    ("candles_1h", "1 day", "1 hour", "1 hour"),
)


def upgrade() -> None:
    """Create the two continuous aggregates and their refresh policies.

    These are the recomputed-from-``trades`` reference for the worker's
    in-memory candles: they survive restarts, absorb late trades, and give
    the hourly series for free by stacking on the minute one. Timescale
    refuses to create a continuous aggregate inside a transaction, hence the
    ``autocommit_block``; each statement commits on its own, so a failure
    midway leaves earlier statements applied. ``downgrade`` is written to
    cope with that (``IF EXISTS``).

    ``WITH NO DATA`` keeps the migration fast on a large table; the policy's
    first run backfills ``start_offset`` worth of history.
    """
    with op.get_context().autocommit_block():
        op.execute(CANDLES_1M)
        op.execute(CANDLES_1H)
        op.execute("""ALTER MATERIALIZED VIEW candles_1h SET (timescaledb.materialized_only = false);""")
        op.execute("""ALTER MATERIALIZED VIEW candles_1m SET (timescaledb.materialized_only = false);""")
        for view, start, end, every in POLICIES:
            op.execute(
                f"SELECT add_continuous_aggregate_policy('{view}', "
                f"start_offset => INTERVAL '{start}', "
                f"end_offset => INTERVAL '{end}', "
                f"schedule_interval => INTERVAL '{every}')"
            )


def downgrade() -> None:
    """Drop the aggregates, hourly first since it depends on the minute one.

    Dropping a continuous aggregate removes its refresh policy with it.
    """
    with op.get_context().autocommit_block():
        op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1h")
        op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1m")
