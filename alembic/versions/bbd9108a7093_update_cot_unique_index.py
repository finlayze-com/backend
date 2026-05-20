"""update cot unique index

Revision ID: bbd9108a7093
Revises: 02826d25f2fd
Create Date: 2026-05-18 10:10:30.603866

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bbd9108a7093'
down_revision: Union[str, Sequence[str], None] = '02826d25f2fd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
