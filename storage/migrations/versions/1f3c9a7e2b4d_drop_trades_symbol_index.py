"""drop trades symbol index

Revision ID: 1f3c9a7e2b4d
Revises: af9cd18d7f88
Create Date: 2026-10-01 13:16:24.878214

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1f3c9a7e2b4d"
down_revision: str | Sequence[str] | None = "af9cd18d7f88"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Every read of trades is time-bounded (aggregate refreshes and the real-time
# tail of candles_1m), and a standalone (symbol) index cannot serve "symbol X
# in time range Y", so it only cost write throughput. If the real-time tail
# ever needs help under multi-pair load, the index to add is a composite
# (symbol, ts_event DESC), not this one. On a hypertable the drop propagates
# to every chunk's copy of the index.


def upgrade() -> None:
    op.drop_index("ix_trades_symbol", table_name="trades")


def downgrade() -> None:
    op.create_index("ix_trades_symbol", "trades", ["symbol"], unique=False)
