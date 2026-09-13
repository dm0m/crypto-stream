"""add side to trades

Revision ID: 340ee63d44e8
Revises: 5b99070d35b3
Create Date: 2026-06-11 17:26:15.692676

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "340ee63d44e8"
down_revision: str | Sequence[str] | None = "5b99070d35b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the NOT NULL ``side`` column backed by a new ``side`` enum type.

    The enum type is created explicitly before the column because
    ``add_column`` alone does not emit ``CREATE TYPE`` on Postgres. There is
    no server default, so this only succeeds on an empty table.
    """
    side_enum = sa.Enum("BUY", "SELL", name="side")
    side_enum.create(op.get_bind())
    op.add_column("trades", sa.Column("side", side_enum, nullable=False))


def downgrade() -> None:
    """Drop the ``side`` column and then the ``side`` enum type it depended on."""
    op.drop_column("trades", "side")
    sa.Enum(name="side").drop(op.get_bind())
