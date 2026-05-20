"""create cot base views

Revision ID: d3b77bc72fea
Revises: 52b37d696128
Create Date: 2026-05-20 17:17:13.214882

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3b77bc72fea'
down_revision: Union[str, Sequence[str], None] = '52b37d696128'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE OR REPLACE VIEW public.vw_cot_base_metrics AS
    SELECT
        id,
        market_and_exchange_names,
        cftc_contract_market_code,
        cftc_market_code,
        cftc_region_code,
        cftc_commodity_code,
        report_date_as_yyyy_mm_dd,
        as_of_date_in_form_yymmdd,
        futonly_or_combined,
        open_interest_all,

        prod_merc_positions_long_all AS commercial_long,
        prod_merc_positions_short_all AS commercial_short,
        prod_merc_positions_long_all - prod_merc_positions_short_all AS commercial_net,
        change_in_prod_merc_long_all - change_in_prod_merc_short_all AS commercial_net_change,

        swap_positions_long_all AS swap_dealers_long,
        swap_positions_short_all AS swap_dealers_short,
        swap_positions_spread_all AS swap_dealers_spread,
        swap_positions_long_all - swap_positions_short_all AS swap_dealers_net,
        change_in_swap_long_all - change_in_swap_short_all AS swap_dealers_net_change,

        m_money_positions_long_all AS managed_money_long,
        m_money_positions_short_all AS managed_money_short,
        m_money_positions_spread_all AS managed_money_spread,
        m_money_positions_long_all - m_money_positions_short_all AS managed_money_net,
        change_in_m_money_long_all - change_in_m_money_short_all AS managed_money_net_change,

        other_rept_positions_long_all AS other_reportables_long,
        other_rept_positions_short_all AS other_reportables_short,
        other_rept_positions_spread_all AS other_reportables_spread,
        other_rept_positions_long_all - other_rept_positions_short_all AS other_reportables_net,
        change_in_other_rept_long_all - change_in_other_rept_short_all AS other_reportables_net_change,

        nonrept_positions_long_all AS nonreportable_long,
        nonrept_positions_short_all AS nonreportable_short,
        nonrept_positions_long_all - nonrept_positions_short_all AS nonreportable_net,
        change_in_nonrept_long_all - change_in_nonrept_short_all AS nonreportable_net_change,

        pct_of_oi_prod_merc_long_all AS commercial_long_pct_oi,
        pct_of_oi_prod_merc_short_all AS commercial_short_pct_oi,
        pct_of_oi_swap_long_all AS swap_dealers_long_pct_oi,
        pct_of_oi_swap_short_all AS swap_dealers_short_pct_oi,
        pct_of_oi_m_money_long_all AS managed_money_long_pct_oi,
        pct_of_oi_m_money_short_all AS managed_money_short_pct_oi,
        pct_of_oi_other_rept_long_all AS other_reportables_long_pct_oi,
        pct_of_oi_other_rept_short_all AS other_reportables_short_pct_oi,
        pct_of_oi_nonrept_long_all AS nonreportable_long_pct_oi,
        pct_of_oi_nonrept_short_all AS nonreportable_short_pct_oi,

        ROUND(
            100.0 * (m_money_positions_long_all - m_money_positions_short_all)
            / NULLIF(open_interest_all, 0),
            2
        ) AS managed_money_net_pct_oi,

        ROUND(
            100.0 * (prod_merc_positions_long_all - prod_merc_positions_short_all)
            / NULLIF(open_interest_all, 0),
            2
        ) AS commercial_net_pct_oi,

        ROUND(
            100.0 * (swap_positions_long_all - swap_positions_short_all)
            / NULLIF(open_interest_all, 0),
            2
        ) AS swap_dealers_net_pct_oi,

        ROUND(
            100.0 * (other_rept_positions_long_all - other_rept_positions_short_all)
            / NULLIF(open_interest_all, 0),
            2
        ) AS other_reportables_net_pct_oi,

        ROUND(
            100.0 * (nonrept_positions_long_all - nonrept_positions_short_all)
            / NULLIF(open_interest_all, 0),
            2
        ) AS nonreportable_net_pct_oi

    FROM public.cot_disaggregated_futures_only;
    """)

    op.execute("""
    CREATE OR REPLACE VIEW public.vw_cot_latest AS
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cftc_contract_market_code,
                    cftc_market_code,
                    cftc_commodity_code,
                    futonly_or_combined
                ORDER BY report_date_as_yyyy_mm_dd DESC
            ) AS rn
        FROM public.vw_cot_base_metrics b
    ) t
    WHERE rn = 1;
    """)


def downgrade():
    op.execute("DROP VIEW IF EXISTS public.vw_cot_latest;")
    op.execute("DROP VIEW IF EXISTS public.vw_cot_base_metrics;")