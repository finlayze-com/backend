"""create_mv_symbol_market_map

Revision ID: 94aac37da68a
Revises: 42a3db6c2fcf
Create Date: 2026-02-09 10:50:26.784815

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '94aac37da68a'
down_revision: Union[str, Sequence[str], None] = '42a3db6c2fcf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None




def upgrade():
    # 1) drop old objects if any (in case previous run created MV but failed on index)
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_symbol_market_map CASCADE;
    """)

    # 2) create MV
    op.execute("""
    CREATE MATERIALIZED VIEW mv_symbol_market_map AS
    SELECT
      sd."insCode"::bigint AS "insCode",
      sd."stock_ticker" AS stock_ticker,

      regexp_replace(
        replace(replace(replace(trim(lower(sd."stock_ticker")), 'ي','ی'),'ك','ک'), chr(8204), ''),
        '\\s+','', 'g'
      ) AS ticker_key,

      COALESCE(NULLIF(trim(sd."sector"), ''), 'other') AS sector,
      COALESCE(NULLIF(trim(sd."market"), ''), 'other') AS market
    FROM public.symboldetail sd
    WHERE sd."insCode" IS NOT NULL;
    """)

    # 3) indexes
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_symbol_market_map_inscode
    ON mv_symbol_market_map ("insCode");
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_ticker_key
    ON mv_symbol_market_map (ticker_key);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_market
    ON mv_symbol_market_map (market);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_sector
    ON mv_symbol_market_map (sector);
    """)


def downgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_symbol_market_map CASCADE;
    """)
