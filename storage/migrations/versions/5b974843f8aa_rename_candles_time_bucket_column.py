"""rename candles time_bucket column

Revision ID: 5b974843f8aa
Revises: 76b79c8c11c8
Create Date: 2026-09-23 15:30:04.165006

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5b974843f8aa"
down_revision: str | Sequence[str] | None = "76b79c8c11c8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename the aggregates' ``bucket`` column to ``ts_open``.

    Lines the two continuous aggregates up with ``candles`` and
    ``domain.Candle``, which already call a bucket start ``ts_open``. Once
    the names match, one repository method can build a ``Candle`` from
    either relation, so which of them the API reads becomes a one-line
    change rather than a refactor of every layer above it.

    Timescale accepts the rename on a continuous aggregate, and the
    hierarchical ``candles_1h`` keeps resolving afterwards because Postgres
    tracks its reference to ``candles_1m`` by column identity rather than by
    name. Unlike the statements in ``f75fde4ffc71`` this needs no
    ``autocommit_block``: a rename is ordinary DDL and runs inside the
    migration's transaction.
    """
    op.alter_column("candles_1m", "bucket", new_column_name="ts_open")
    op.alter_column("candles_1h", "bucket", new_column_name="ts_open")


def downgrade() -> None:
    """Restore the ``bucket`` name the aggregates were created with."""
    op.alter_column("candles_1m", "ts_open", new_column_name="bucket")
    op.alter_column("candles_1h", "ts_open", new_column_name="bucket")
