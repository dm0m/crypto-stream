"""trades retention and compression policies

Revision ID: 76b79c8c11c8
Revises: f75fde4ffc71
Create Date: 2026-09-16 21:30:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "76b79c8c11c8"
down_revision: str | Sequence[str] | None = "f75fde4ffc71"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Hot/warm/cold lifecycle for raw ticks. Candles keep the long-term history;
# the ticks themselves are only needed for a week of reprocessing headroom.
RETENTION = "7 days"
COMPRESS_AFTER = "1 day"


def upgrade() -> None:
    """Compress ``trades`` chunks older than a day and drop them after a week.

    Compression groups rows by ``(exchange, symbol)`` and orders them by
    ``ts_event, trade_id`` inside each group. Together those columns cover
    the primary key, which Timescale requires so that the worker's
    ``ON CONFLICT DO NOTHING`` keeps working on compressed chunks. The
    ordering also mirrors the key, so time-range reads on old chunks stay
    cheap.

    Retention at seven days is deliberately longer than the continuous
    aggregates' ``start_offset`` (one hour and one day): a refresh only
    recomputes ranges whose raw rows still exist. Shorten one and you must
    shorten the other.
    """
    op.execute(
        "ALTER TABLE trades SET ("
        "timescaledb.compress, "
        "timescaledb.compress_segmentby = 'exchange, symbol', "
        "timescaledb.compress_orderby = 'ts_event, trade_id')"
    )
    op.execute(f"SELECT add_compression_policy('trades', INTERVAL '{COMPRESS_AFTER}')")
    op.execute(f"SELECT add_retention_policy('trades', INTERVAL '{RETENTION}')")


def downgrade() -> None:
    """Remove both policies and disable compression on ``trades``.

    Chunks already compressed are decompressed first; ``ALTER TABLE ... SET
    (timescaledb.compress = false)`` refuses while any remain.
    """
    op.execute("SELECT remove_retention_policy('trades', if_exists => true)")
    op.execute("SELECT remove_compression_policy('trades', if_exists => true)")
    op.execute(
        "SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks('trades') c"
    )
    op.execute("ALTER TABLE trades SET (timescaledb.compress = false)")
