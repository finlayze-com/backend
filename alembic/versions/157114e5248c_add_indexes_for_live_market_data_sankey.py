"""add indexes for live_market_data sankey

Revision ID: 157114e5248c
Revises: 606d90ebfeef
Create Date: 2025-11-16 17:30:36.002083

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '157114e5248c'
down_revision: Union[str, Sequence[str], None] = '606d90ebfeef'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ۱) ایندکس روی تاریخِ updated_at (به صورت expression)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_updatedat_date
        ON live_market_data (( "updated_at"::date ));
    """)

    # ۲) ایندکس روی (Ticker, updated_at)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_ticker_updatedat
        ON live_market_data ("Ticker", "updated_at");
    """)

    # ۳) ایندکس روی (Sector, updated_at) برای حالت intra-sector
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_sector_updatedat
        ON live_market_data ("Sector", "updated_at");
    """)


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS idx_lmd_sector_updatedat;')
    op.execute('DROP INDEX IF EXISTS idx_lmd_ticker_updatedat;')
    op.execute('DROP INDEX IF EXISTS idx_lmd_updatedat_date;')