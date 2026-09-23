"""create candles hypertable

Revision ID: fd60264121ba
Revises: c28aa8e6d6b8
Create Date: 2026-09-16 10:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

# revision identifiers, used by Alembic.
revision: str = "fd60264121ba"
down_revision: str | Sequence[str] | None = "c28aa8e6d6b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create ``candles`` with its ``candle_interval`` enum and make it a hypertable.

    ``create_table`` emits ``CREATE TYPE`` for the new enum itself (unlike
    ``add_column`` in the ``side`` revision), so there is no explicit create.
    The type is not named ``interval`` because Postgres has a built-in type
    of that name.

    The primary key leads with ``ts_open`` so the table is hypertable-legal
    from the start. Timescale's default ``(ts_open DESC)`` index is disabled
    because the key index already serves time-range scans; one fewer index
    per chunk to maintain on every write. The ``exchange`` enum type already
    exists from the first revision and is reused, not recreated.
    """
    op.create_table(
        "candles",
        sa.Column("ts_open", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exchange", ENUM(name="exchange", create_type=False), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
        sa.Column(
            "interval", sa.Enum("M1", "H1", name="candle_interval"), nullable=False
        ),
        sa.Column("open", sa.Numeric(), nullable=False),
        sa.Column("high", sa.Numeric(), nullable=False),
        sa.Column("low", sa.Numeric(), nullable=False),
        sa.Column("close", sa.Numeric(), nullable=False),
        sa.Column("volume", sa.Numeric(), nullable=False),
        sa.Column("trade_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("ts_open", "exchange", "symbol", "interval"),
    )
    op.execute(
        "SELECT create_hypertable('candles', by_range('ts_open'), create_default_indexes => false)"
    )


def downgrade() -> None:
    """Drop the hypertable (chunks included) and then the ``candle_interval`` enum type."""
    op.drop_table("candles")
    sa.Enum(name="candle_interval").drop(op.get_bind())
