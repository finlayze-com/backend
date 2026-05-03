"""create vw_option_iv_hv_enriched

Revision ID: 1a330715e9e4
Revises: 99b47298b77e
Create Date: 2026-04-26 12:58:01.064370

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1a330715e9e4'
down_revision: Union[str, Sequence[str], None] = '99b47298b77e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE OR REPLACE VIEW public.vw_option_iv_hv_enriched AS
    SELECT
        og.*,

        uv.hv_10d,
        uv.hv_20d,
        uv.hv_30d,
        uv.hv_60d,

        -- ratios
        CASE 
            WHEN uv.hv_10d IS NOT NULL AND uv.hv_10d <> 0
            THEN og.iv / uv.hv_10d
        END AS iv_hv_10d_ratio,

        CASE 
            WHEN uv.hv_20d IS NOT NULL AND uv.hv_20d <> 0
            THEN og.iv / uv.hv_20d
        END AS iv_hv_20d_ratio,

        CASE 
            WHEN uv.hv_30d IS NOT NULL AND uv.hv_30d <> 0
            THEN og.iv / uv.hv_30d
        END AS iv_hv_30d_ratio,

        CASE 
            WHEN uv.hv_60d IS NOT NULL AND uv.hv_60d <> 0
            THEN og.iv / uv.hv_60d
        END AS iv_hv_60d_ratio

    FROM public.option_greeks_daily og
    LEFT JOIN public.underlying_volatility_daily uv
        ON uv.ticker = og.underlying_ticker
       AND uv.trade_date = og.trade_date
    WHERE og.iv IS NOT NULL;
    """)


def downgrade():
    op.execute("DROP VIEW IF EXISTS public.vw_option_iv_hv_enriched;")