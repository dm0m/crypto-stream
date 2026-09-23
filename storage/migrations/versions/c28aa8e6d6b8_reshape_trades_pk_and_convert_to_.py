"""reshape trades pk and convert to hypertable

Revision ID: c28aa8e6d6b8
Revises: 340ee63d44e8
Create Date: 2026-09-14 21:52:23.785785

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM

# revision identifiers, used by Alembic.
revision: str = "c28aa8e6d6b8"
down_revision: str | Sequence[str] | None = "340ee63d44e8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE_NAME = "trades"
DATA_COLUMNS = (
    "price",
    "quantity",
    "ts_event",
    "ts_ingest",
    "exchange",
    "trade_id",
    "side",
    "symbol",
)


def upgrade() -> None:
    """Replace the surrogate key with a time-led composite key, then partition.

    TimescaleDB requires every unique index on a hypertable to include the
    partition column, so the ``id`` primary key and the ``(exchange,
    trade_id)`` unique constraint are both replaced by a single primary key
    on ``(ts_event, exchange, trade_id)``. Dedup semantics are unchanged: a
    redelivered trade carries the same ``ts_event``. The standalone
    ``trade_id`` index goes too, since the app never looks trades up by id
    alone.

    ``migrate_data => true`` lets the conversion succeed on a table that
    already holds rows; on an empty table it is a no-op.
    """
    op.drop_constraint("trades_exchange_trade_id_key", TABLE_NAME, type_="unique")
    op.drop_index("ix_trades_trade_id", TABLE_NAME)
    op.drop_constraint("trades_pkey", TABLE_NAME, type_="primary")
    op.drop_column(TABLE_NAME, "id")
    op.create_primary_key(
        "trades_pkey", TABLE_NAME, ["ts_event", "exchange", "trade_id"]
    )

    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    op.execute(
        "SELECT create_hypertable('trades', by_range('ts_event'), migrate_data => true)"
    )


def downgrade() -> None:
    """Rebuild ``trades`` as a plain table with the pre-hypertable shape.

    There is no ``drop_hypertable``, so the rows are copied into a fresh
    plain table, the hypertable (and all its chunks) is dropped, and the copy
    is renamed into place. Surrogate ``id`` values are regenerated; nothing
    referenced the old ones. Constraint and index names match the ones the
    earlier revisions created so their own ``downgrade`` still finds them.

    The extension is left installed: ``upgrade`` uses ``IF NOT EXISTS`` and
    later revisions may depend on it. This rewrites the whole table, so it
    is slow on large datasets.
    """
    plain = f"{TABLE_NAME}_plain"
    op.create_table(
        plain,
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("quantity", sa.Numeric(), nullable=False),
        sa.Column("ts_event", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ts_ingest", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "exchange",
            ENUM(name="exchange", create_type=False),
            nullable=False,
        ),
        sa.Column("trade_id", sa.String(length=20), nullable=False),
        sa.Column("side", ENUM(name="side", create_type=False), nullable=False),
        sa.Column("symbol", sa.String(length=20), nullable=False),
    )

    columns = ", ".join(DATA_COLUMNS)
    op.execute(
        f"INSERT INTO {plain} ({columns}) "
        f"SELECT {columns} FROM {TABLE_NAME} ORDER BY ts_event"
    )

    op.drop_table(TABLE_NAME)
    op.rename_table(plain, TABLE_NAME)

    op.create_primary_key("trades_pkey", TABLE_NAME, ["id"])
    op.create_unique_constraint(
        "trades_exchange_trade_id_key", TABLE_NAME, ["exchange", "trade_id"]
    )
    op.create_index("ix_trades_symbol", TABLE_NAME, ["symbol"], unique=False)
    op.create_index("ix_trades_trade_id", TABLE_NAME, ["trade_id"], unique=False)
