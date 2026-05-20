"""create cot positioning score view

Revision ID: b55f3bc45c26
Revises: d3b77bc72fea
Create Date: 2026-05-20 17:28:30.022236

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b55f3bc45c26'
down_revision: Union[str, Sequence[str], None] = 'd3b77bc72fea'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    op.execute("""
    CREATE OR REPLACE VIEW public.vw_cot_positioning_score AS
    WITH base AS (
        SELECT
            b.*,

            AVG(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ) AS mm_net_avg_52w,

            STDDEV_SAMP(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 51 PRECEDING AND CURRENT ROW
            ) AS mm_net_std_52w,

            AVG(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 155 PRECEDING AND CURRENT ROW
            ) AS mm_net_avg_156w,

            STDDEV_SAMP(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 155 PRECEDING AND CURRENT ROW
            ) AS mm_net_std_156w,

            AVG(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 259 PRECEDING AND CURRENT ROW
            ) AS mm_net_avg_260w,

            STDDEV_SAMP(managed_money_net) OVER (
                PARTITION BY cftc_contract_market_code, futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd
                ROWS BETWEEN 259 PRECEDING AND CURRENT ROW
            ) AS mm_net_std_260w

        FROM public.vw_cot_base_metrics b
    ),

    scored AS (
        SELECT
            base.*,

            ROUND(
                (managed_money_net - mm_net_avg_52w)
                / NULLIF(mm_net_std_52w, 0),
                3
            ) AS managed_money_net_z_52w,

            ROUND(
                (managed_money_net - mm_net_avg_156w)
                / NULLIF(mm_net_std_156w, 0),
                3
            ) AS managed_money_net_z_156w,

            ROUND(
                (managed_money_net - mm_net_avg_260w)
                / NULLIF(mm_net_std_260w, 0),
                3
            ) AS managed_money_net_z_260w

        FROM base
    )

    SELECT
        scored.*,

        CASE
            WHEN managed_money_net_z_52w >= 2 THEN 'extreme_long'
            WHEN managed_money_net_z_52w >= 1 THEN 'long'
            WHEN managed_money_net_z_52w <= -2 THEN 'extreme_short'
            WHEN managed_money_net_z_52w <= -1 THEN 'short'
            ELSE 'neutral'
        END AS crowding_state_52w,

        CASE
            WHEN managed_money_net_z_156w >= 2 THEN 'extreme_long'
            WHEN managed_money_net_z_156w >= 1 THEN 'long'
            WHEN managed_money_net_z_156w <= -2 THEN 'extreme_short'
            WHEN managed_money_net_z_156w <= -1 THEN 'short'
            ELSE 'neutral'
        END AS crowding_state_156w,

        CASE
            WHEN managed_money_net_z_260w >= 2 THEN 'extreme_long'
            WHEN managed_money_net_z_260w >= 1 THEN 'long'
            WHEN managed_money_net_z_260w <= -2 THEN 'extreme_short'
            WHEN managed_money_net_z_260w <= -1 THEN 'short'
            ELSE 'neutral'
        END AS crowding_state_260w,

        ROUND(
            (
                COALESCE(managed_money_net_z_52w, 0) * 0.5
                + COALESCE(managed_money_net_z_156w, 0) * 0.3
                + COALESCE(managed_money_net_z_260w, 0) * 0.2
            ),
            3
        ) AS crowding_score,

        CASE
            WHEN (
                COALESCE(managed_money_net_z_52w, 0) * 0.5
                + COALESCE(managed_money_net_z_156w, 0) * 0.3
                + COALESCE(managed_money_net_z_260w, 0) * 0.2
            ) >= 2 THEN 'crowded_long_squeeze_risk'

            WHEN (
                COALESCE(managed_money_net_z_52w, 0) * 0.5
                + COALESCE(managed_money_net_z_156w, 0) * 0.3
                + COALESCE(managed_money_net_z_260w, 0) * 0.2
            ) <= -2 THEN 'crowded_short_reversal_risk'

            WHEN (
                COALESCE(managed_money_net_z_52w, 0) * 0.5
                + COALESCE(managed_money_net_z_156w, 0) * 0.3
                + COALESCE(managed_money_net_z_260w, 0) * 0.2
            ) >= 1 THEN 'long_positioning'

            WHEN (
                COALESCE(managed_money_net_z_52w, 0) * 0.5
                + COALESCE(managed_money_net_z_156w, 0) * 0.3
                + COALESCE(managed_money_net_z_260w, 0) * 0.2
            ) <= -1 THEN 'short_positioning'

            ELSE 'neutral'
        END AS positioning_bias

    FROM scored;
    """)


def downgrade():
    op.execute("DROP VIEW IF EXISTS public.vw_cot_positioning_score;")