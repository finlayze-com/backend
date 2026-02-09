"""create mv_orderbook_report

Revision ID: 2e0542b2db24
Revises: 33f4266ac023
Create Date: 2026-02-08 08:21:02.816152

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2e0542b2db24'
down_revision: Union[str, Sequence[str], None] = '33f4266ac023'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
