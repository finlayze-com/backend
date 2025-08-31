"""add per-tool daily indicators tables

Revision ID: 283669dbe259
Revises: b62cbb14a9af
Create Date: 2025-08-28 09:28:30.899161

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '283669dbe259'
down_revision: Union[str, Sequence[str], None] = 'b62cbb14a9af'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
