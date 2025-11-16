"""add live_market_data indexes for sankey v2

Revision ID: 9880629f91e7
Revises: 606d90ebfeef
Create Date: 2025-11-16 17:53:17.363317

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9880629f91e7'
down_revision: Union[str, Sequence[str], None] = '606d90ebfeef'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) ایندکس روی تاریخ updated_at
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_updatedat_date
        ON live_market_data (( "updated_at"::date ));
    """)

    # 2) ایندکس برای DISTINCT ON در سطح کل بازار
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_ticker_updatedat
        ON live_market_data ("Ticker", "updated_at");
    """)

    # 3) ایندکس برای حالت intra-sector
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_lmd_sector_updatedat
        ON live_market_data ("Sector", "updated_at");
    """)


def downgrade() -> None:
    op.execute('DROP INDEX IF EXISTS idx_lmd_sector_updatedat;')
    op.execute('DROP INDEX IF EXISTS idx_lmd_ticker_updatedat;')
    op.execute('DROP INDEX IF EXISTS idx_lmd_updatedat_date;')
