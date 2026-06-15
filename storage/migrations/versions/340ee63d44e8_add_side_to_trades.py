"""add side to trades

Revision ID: 340ee63d44e8
Revises: 5b99070d35b3
Create Date: 2026-06-11 17:26:15.692676

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '340ee63d44e8'
down_revision: Union[str, Sequence[str], None] = '5b99070d35b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    side_enum = sa.Enum("BUY", "SELL", name="side")
    side_enum.create(op.get_bind())
    op.add_column("trades", sa.Column("side", side_enum, nullable=False))

def downgrade() -> None:
    op.drop_column("trades", "side")
    sa.Enum(name="side").drop(op.get_bind())
