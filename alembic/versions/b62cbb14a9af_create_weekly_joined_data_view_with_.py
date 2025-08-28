"""create weekly_joined_data view with symbol_id

Revision ID: b62cbb14a9af
Revises: 9a8b824a1394
Create Date: 2025-08-27 12:23:39.709446

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b62cbb14a9af'
down_revision: Union[str, Sequence[str], None] = '9a8b824a1394'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
