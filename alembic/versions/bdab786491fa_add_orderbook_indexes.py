"""add orderbook indexes

Revision ID: bdab786491fa
Revises: 40b46e7f3851
Create Date: 2025-11-20 15:51:19.721844

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'bdab786491fa'
down_revision: Union[str, Sequence[str], None] = '40b46e7f3851'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1) ایندکس روی زمان
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderbook_snapshot_ts
        ON orderbook_snapshot ("Timestamp");
    """)

    # 2) ایندکس روی سکتور/سیمبل/زمان برای اینترا سکتور
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderbook_snapshot_sector_symbol_ts
        ON orderbook_snapshot ("Sector","Symbol","Timestamp");
    """)


def downgrade():
    op.execute('DROP INDEX IF EXISTS idx_orderbook_snapshot_sector_symbol_ts;')
    op.execute('DROP INDEX IF EXISTS idx_orderbook_snapshot_ts;')