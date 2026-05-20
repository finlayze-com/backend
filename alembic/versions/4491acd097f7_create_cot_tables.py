"""create cot raw futures only table

Revision ID: 4491acd097f7
Revises: a4fb5455d4b8
Create Date: 2026-05-17 13:42:29.736029
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "4491acd097f7"
down_revision: Union[str, Sequence[str], None] = "a4fb5455d4b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

"""create raw disaggregated cot historical table

Revision ID: 4491acd097f7
Revises: a4fb5455d4b8
Create Date: 2026-05-17 13:42:29.736029
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "4491acd097f7"
down_revision: Union[str, Sequence[str], None] = "a4fb5455d4b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "cot_disaggregated_futures_only",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("market_and_exchange_names", sa.Text(), nullable=False),  # Market_and_Exchange_Names
        sa.Column("as_of_date_in_form_yymmdd", sa.Integer(), nullable=True),  # As_of_Date_In_Form_YYMMDD
        sa.Column("report_date_as_yyyy_mm_dd", sa.Date(), nullable=False),  # Report_Date_as_YYYY-MM-DD
        sa.Column("cftc_contract_market_code", sa.String(length=50), nullable=False),  # CFTC_Contract_Market_Code
        sa.Column("cftc_market_code", sa.String(length=50), nullable=True),  # CFTC_Market_Code
        sa.Column("cftc_region_code", sa.String(length=50), nullable=True),  # CFTC_Region_Code
        sa.Column("cftc_commodity_code", sa.String(length=50), nullable=False),  # CFTC_Commodity_Code
        sa.Column("open_interest_all", sa.BigInteger(), nullable=True),  # Open_Interest_All
        sa.Column("prod_merc_positions_long_all", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Long_All
        sa.Column("prod_merc_positions_short_all", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Short_All
        sa.Column("swap_positions_long_all", sa.BigInteger(), nullable=True),  # Swap_Positions_Long_All
        sa.Column("swap_positions_short_all", sa.BigInteger(), nullable=True),  # Swap__Positions_Short_All
        sa.Column("swap_positions_spread_all", sa.BigInteger(), nullable=True),  # Swap__Positions_Spread_All
        sa.Column("m_money_positions_long_all", sa.BigInteger(), nullable=True),  # M_Money_Positions_Long_All
        sa.Column("m_money_positions_short_all", sa.BigInteger(), nullable=True),  # M_Money_Positions_Short_All
        sa.Column("m_money_positions_spread_all", sa.BigInteger(), nullable=True),  # M_Money_Positions_Spread_All
        sa.Column("other_rept_positions_long_all", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Long_All
        sa.Column("other_rept_positions_short_all", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Short_All
        sa.Column("other_rept_positions_spread_all", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Spread_All
        sa.Column("tot_rept_positions_long_all", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Long_All
        sa.Column("tot_rept_positions_short_all", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Short_All
        sa.Column("nonrept_positions_long_all", sa.BigInteger(), nullable=True),  # NonRept_Positions_Long_All
        sa.Column("nonrept_positions_short_all", sa.BigInteger(), nullable=True),  # NonRept_Positions_Short_All
        sa.Column("open_interest_old", sa.BigInteger(), nullable=True),  # Open_Interest_Old
        sa.Column("prod_merc_positions_long_old", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Long_Old
        sa.Column("prod_merc_positions_short_old", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Short_Old
        sa.Column("swap_positions_long_old", sa.BigInteger(), nullable=True),  # Swap_Positions_Long_Old
        sa.Column("swap_positions_short_old", sa.BigInteger(), nullable=True),  # Swap__Positions_Short_Old
        sa.Column("swap_positions_spread_old", sa.BigInteger(), nullable=True),  # Swap__Positions_Spread_Old
        sa.Column("m_money_positions_long_old", sa.BigInteger(), nullable=True),  # M_Money_Positions_Long_Old
        sa.Column("m_money_positions_short_old", sa.BigInteger(), nullable=True),  # M_Money_Positions_Short_Old
        sa.Column("m_money_positions_spread_old", sa.BigInteger(), nullable=True),  # M_Money_Positions_Spread_Old
        sa.Column("other_rept_positions_long_old", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Long_Old
        sa.Column("other_rept_positions_short_old", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Short_Old
        sa.Column("other_rept_positions_spread_old", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Spread_Old
        sa.Column("tot_rept_positions_long_old", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Long_Old
        sa.Column("tot_rept_positions_short_old", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Short_Old
        sa.Column("nonrept_positions_long_old", sa.BigInteger(), nullable=True),  # NonRept_Positions_Long_Old
        sa.Column("nonrept_positions_short_old", sa.BigInteger(), nullable=True),  # NonRept_Positions_Short_Old
        sa.Column("open_interest_other", sa.BigInteger(), nullable=True),  # Open_Interest_Other
        sa.Column("prod_merc_positions_long_other", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Long_Other
        sa.Column("prod_merc_positions_short_other", sa.BigInteger(), nullable=True),  # Prod_Merc_Positions_Short_Other
        sa.Column("swap_positions_long_other", sa.BigInteger(), nullable=True),  # Swap_Positions_Long_Other
        sa.Column("swap_positions_short_other", sa.BigInteger(), nullable=True),  # Swap__Positions_Short_Other
        sa.Column("swap_positions_spread_other", sa.BigInteger(), nullable=True),  # Swap__Positions_Spread_Other
        sa.Column("m_money_positions_long_other", sa.BigInteger(), nullable=True),  # M_Money_Positions_Long_Other
        sa.Column("m_money_positions_short_other", sa.BigInteger(), nullable=True),  # M_Money_Positions_Short_Other
        sa.Column("m_money_positions_spread_other", sa.BigInteger(), nullable=True),  # M_Money_Positions_Spread_Other
        sa.Column("other_rept_positions_long_other", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Long_Other
        sa.Column("other_rept_positions_short_other", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Short_Other
        sa.Column("other_rept_positions_spread_other", sa.BigInteger(), nullable=True),  # Other_Rept_Positions_Spread_Other
        sa.Column("tot_rept_positions_long_other", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Long_Other
        sa.Column("tot_rept_positions_short_other", sa.BigInteger(), nullable=True),  # Tot_Rept_Positions_Short_Other
        sa.Column("nonrept_positions_long_other", sa.BigInteger(), nullable=True),  # NonRept_Positions_Long_Other
        sa.Column("nonrept_positions_short_other", sa.BigInteger(), nullable=True),  # NonRept_Positions_Short_Other
        sa.Column("change_in_open_interest_all", sa.BigInteger(), nullable=True),  # Change_in_Open_Interest_All
        sa.Column("change_in_prod_merc_long_all", sa.BigInteger(), nullable=True),  # Change_in_Prod_Merc_Long_All
        sa.Column("change_in_prod_merc_short_all", sa.BigInteger(), nullable=True),  # Change_in_Prod_Merc_Short_All
        sa.Column("change_in_swap_long_all", sa.BigInteger(), nullable=True),  # Change_in_Swap_Long_All
        sa.Column("change_in_swap_short_all", sa.BigInteger(), nullable=True),  # Change_in_Swap_Short_All
        sa.Column("change_in_swap_spread_all", sa.BigInteger(), nullable=True),  # Change_in_Swap_Spread_All
        sa.Column("change_in_m_money_long_all", sa.BigInteger(), nullable=True),  # Change_in_M_Money_Long_All
        sa.Column("change_in_m_money_short_all", sa.BigInteger(), nullable=True),  # Change_in_M_Money_Short_All
        sa.Column("change_in_m_money_spread_all", sa.BigInteger(), nullable=True),  # Change_in_M_Money_Spread_All
        sa.Column("change_in_other_rept_long_all", sa.BigInteger(), nullable=True),  # Change_in_Other_Rept_Long_All
        sa.Column("change_in_other_rept_short_all", sa.BigInteger(), nullable=True),  # Change_in_Other_Rept_Short_All
        sa.Column("change_in_other_rept_spread_all", sa.BigInteger(), nullable=True),  # Change_in_Other_Rept_Spread_All
        sa.Column("change_in_tot_rept_long_all", sa.BigInteger(), nullable=True),  # Change_in_Tot_Rept_Long_All
        sa.Column("change_in_tot_rept_short_all", sa.BigInteger(), nullable=True),  # Change_in_Tot_Rept_Short_All
        sa.Column("change_in_nonrept_long_all", sa.BigInteger(), nullable=True),  # Change_in_NonRept_Long_All
        sa.Column("change_in_nonrept_short_all", sa.BigInteger(), nullable=True),  # Change_in_NonRept_Short_All
        sa.Column("pct_of_open_interest_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_Open_Interest_All
        sa.Column("pct_of_oi_prod_merc_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Long_All
        sa.Column("pct_of_oi_prod_merc_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Short_All
        sa.Column("pct_of_oi_swap_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Long_All
        sa.Column("pct_of_oi_swap_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Short_All
        sa.Column("pct_of_oi_swap_spread_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Spread_All
        sa.Column("pct_of_oi_m_money_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Long_All
        sa.Column("pct_of_oi_m_money_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Short_All
        sa.Column("pct_of_oi_m_money_spread_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Spread_All
        sa.Column("pct_of_oi_other_rept_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Long_All
        sa.Column("pct_of_oi_other_rept_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Short_All
        sa.Column("pct_of_oi_other_rept_spread_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Spread_All
        sa.Column("pct_of_oi_tot_rept_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Long_All
        sa.Column("pct_of_oi_tot_rept_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Short_All
        sa.Column("pct_of_oi_nonrept_long_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Long_All
        sa.Column("pct_of_oi_nonrept_short_all", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Short_All
        sa.Column("pct_of_open_interest_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_Open_Interest_Old
        sa.Column("pct_of_oi_prod_merc_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Long_Old
        sa.Column("pct_of_oi_prod_merc_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Short_Old
        sa.Column("pct_of_oi_swap_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Long_Old
        sa.Column("pct_of_oi_swap_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Short_Old
        sa.Column("pct_of_oi_swap_spread_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Spread_Old
        sa.Column("pct_of_oi_m_money_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Long_Old
        sa.Column("pct_of_oi_m_money_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Short_Old
        sa.Column("pct_of_oi_m_money_spread_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Spread_Old
        sa.Column("pct_of_oi_other_rept_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Long_Old
        sa.Column("pct_of_oi_other_rept_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Short_Old
        sa.Column("pct_of_oi_other_rept_spread_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Spread_Old
        sa.Column("pct_of_oi_tot_rept_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Long_Old
        sa.Column("pct_of_oi_tot_rept_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Short_Old
        sa.Column("pct_of_oi_nonrept_long_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Long_Old
        sa.Column("pct_of_oi_nonrept_short_old", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Short_Old
        sa.Column("pct_of_open_interest_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_Open_Interest_Other
        sa.Column("pct_of_oi_prod_merc_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Long_Other
        sa.Column("pct_of_oi_prod_merc_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Prod_Merc_Short_Other
        sa.Column("pct_of_oi_swap_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Long_Other
        sa.Column("pct_of_oi_swap_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Short_Other
        sa.Column("pct_of_oi_swap_spread_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Swap_Spread_Other
        sa.Column("pct_of_oi_m_money_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Long_Other
        sa.Column("pct_of_oi_m_money_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Short_Other
        sa.Column("pct_of_oi_m_money_spread_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_M_Money_Spread_Other
        sa.Column("pct_of_oi_other_rept_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Long_Other
        sa.Column("pct_of_oi_other_rept_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Short_Other
        sa.Column("pct_of_oi_other_rept_spread_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Other_Rept_Spread_Other
        sa.Column("pct_of_oi_tot_rept_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Long_Other
        sa.Column("pct_of_oi_tot_rept_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_Tot_Rept_Short_Other
        sa.Column("pct_of_oi_nonrept_long_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Long_Other
        sa.Column("pct_of_oi_nonrept_short_other", sa.Numeric(10, 4), nullable=True),  # Pct_of_OI_NonRept_Short_Other
        sa.Column("traders_tot_all", sa.Integer(), nullable=True),  # Traders_Tot_All
        sa.Column("traders_prod_merc_long_all", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Long_All
        sa.Column("traders_prod_merc_short_all", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Short_All
        sa.Column("traders_swap_long_all", sa.Integer(), nullable=True),  # Traders_Swap_Long_All
        sa.Column("traders_swap_short_all", sa.Integer(), nullable=True),  # Traders_Swap_Short_All
        sa.Column("traders_swap_spread_all", sa.Integer(), nullable=True),  # Traders_Swap_Spread_All
        sa.Column("traders_m_money_long_all", sa.Integer(), nullable=True),  # Traders_M_Money_Long_All
        sa.Column("traders_m_money_short_all", sa.Integer(), nullable=True),  # Traders_M_Money_Short_All
        sa.Column("traders_m_money_spread_all", sa.Integer(), nullable=True),  # Traders_M_Money_Spread_All
        sa.Column("traders_other_rept_long_all", sa.Integer(), nullable=True),  # Traders_Other_Rept_Long_All
        sa.Column("traders_other_rept_short_all", sa.Integer(), nullable=True),  # Traders_Other_Rept_Short_All
        sa.Column("traders_other_rept_spread_all", sa.Integer(), nullable=True),  # Traders_Other_Rept_Spread_All
        sa.Column("traders_tot_rept_long_all", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Long_All
        sa.Column("traders_tot_rept_short_all", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Short_All
        sa.Column("traders_tot_old", sa.Integer(), nullable=True),  # Traders_Tot_Old
        sa.Column("traders_prod_merc_long_old", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Long_Old
        sa.Column("traders_prod_merc_short_old", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Short_Old
        sa.Column("traders_swap_long_old", sa.Integer(), nullable=True),  # Traders_Swap_Long_Old
        sa.Column("traders_swap_short_old", sa.Integer(), nullable=True),  # Traders_Swap_Short_Old
        sa.Column("traders_swap_spread_old", sa.Integer(), nullable=True),  # Traders_Swap_Spread_Old
        sa.Column("traders_m_money_long_old", sa.Integer(), nullable=True),  # Traders_M_Money_Long_Old
        sa.Column("traders_m_money_short_old", sa.Integer(), nullable=True),  # Traders_M_Money_Short_Old
        sa.Column("traders_m_money_spread_old", sa.Integer(), nullable=True),  # Traders_M_Money_Spread_Old
        sa.Column("traders_other_rept_long_old", sa.Integer(), nullable=True),  # Traders_Other_Rept_Long_Old
        sa.Column("traders_other_rept_short_old", sa.Integer(), nullable=True),  # Traders_Other_Rept_Short_Old
        sa.Column("traders_other_rept_spread_old", sa.Integer(), nullable=True),  # Traders_Other_Rept_Spread_Old
        sa.Column("traders_tot_rept_long_old", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Long_Old
        sa.Column("traders_tot_rept_short_old", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Short_Old
        sa.Column("traders_tot_other", sa.Integer(), nullable=True),  # Traders_Tot_Other
        sa.Column("traders_prod_merc_long_other", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Long_Other
        sa.Column("traders_prod_merc_short_other", sa.Integer(), nullable=True),  # Traders_Prod_Merc_Short_Other
        sa.Column("traders_swap_long_other", sa.Integer(), nullable=True),  # Traders_Swap_Long_Other
        sa.Column("traders_swap_short_other", sa.Integer(), nullable=True),  # Traders_Swap_Short_Other
        sa.Column("traders_swap_spread_other", sa.Integer(), nullable=True),  # Traders_Swap_Spread_Other
        sa.Column("traders_m_money_long_other", sa.Integer(), nullable=True),  # Traders_M_Money_Long_Other
        sa.Column("traders_m_money_short_other", sa.Integer(), nullable=True),  # Traders_M_Money_Short_Other
        sa.Column("traders_m_money_spread_other", sa.Integer(), nullable=True),  # Traders_M_Money_Spread_Other
        sa.Column("traders_other_rept_long_other", sa.Integer(), nullable=True),  # Traders_Other_Rept_Long_Other
        sa.Column("traders_other_rept_short_other", sa.Integer(), nullable=True),  # Traders_Other_Rept_Short_Other
        sa.Column("traders_other_rept_spread_other", sa.Integer(), nullable=True),  # Traders_Other_Rept_Spread_Other
        sa.Column("traders_tot_rept_long_other", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Long_Other
        sa.Column("traders_tot_rept_short_other", sa.Integer(), nullable=True),  # Traders_Tot_Rept_Short_Other
        sa.Column("conc_gross_le_4_tdr_long_all", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Long_All
        sa.Column("conc_gross_le_4_tdr_short_all", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Short_All
        sa.Column("conc_gross_le_8_tdr_long_all", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Long_All
        sa.Column("conc_gross_le_8_tdr_short_all", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Short_All
        sa.Column("conc_net_le_4_tdr_long_all", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Long_All
        sa.Column("conc_net_le_4_tdr_short_all", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Short_All
        sa.Column("conc_net_le_8_tdr_long_all", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Long_All
        sa.Column("conc_net_le_8_tdr_short_all", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Short_All
        sa.Column("conc_gross_le_4_tdr_long_old", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Long_Old
        sa.Column("conc_gross_le_4_tdr_short_old", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Short_Old
        sa.Column("conc_gross_le_8_tdr_long_old", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Long_Old
        sa.Column("conc_gross_le_8_tdr_short_old", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Short_Old
        sa.Column("conc_net_le_4_tdr_long_old", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Long_Old
        sa.Column("conc_net_le_4_tdr_short_old", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Short_Old
        sa.Column("conc_net_le_8_tdr_long_old", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Long_Old
        sa.Column("conc_net_le_8_tdr_short_old", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Short_Old
        sa.Column("conc_gross_le_4_tdr_long_other", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Long_Other
        sa.Column("conc_gross_le_4_tdr_short_other", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_4_TDR_Short_Other
        sa.Column("conc_gross_le_8_tdr_long_other", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Long_Other
        sa.Column("conc_gross_le_8_tdr_short_other", sa.Numeric(10, 4), nullable=True),  # Conc_Gross_LE_8_TDR_Short_Other
        sa.Column("conc_net_le_4_tdr_long_other", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Long_Other
        sa.Column("conc_net_le_4_tdr_short_other", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_4_TDR_Short_Other
        sa.Column("conc_net_le_8_tdr_long_other", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Long_Other
        sa.Column("conc_net_le_8_tdr_short_other", sa.Numeric(10, 4), nullable=True),  # Conc_Net_LE_8_TDR_Short_Other
        sa.Column("contract_units", sa.Text(), nullable=True),  # Contract_Units
        sa.Column("cftc_contract_market_code_quotes", sa.String(length=50), nullable=True),  # CFTC_Contract_Market_Code_Quotes
        sa.Column("cftc_market_code_quotes", sa.String(length=50), nullable=True),  # CFTC_Market_Code_Quotes
        sa.Column("cftc_commodity_code_quotes", sa.String(length=50), nullable=True),  # CFTC_Commodity_Code_Quotes
        sa.Column("cftc_subgroup_code", sa.String(length=50), nullable=True),  # CFTC_SubGroup_Code
        sa.Column("futonly_or_combined", sa.String(length=50), nullable=True),  # FutOnly_or_Combined,
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False),

        sa.UniqueConstraint(
            "cftc_contract_market_code",
            "report_date_as_yyyy_mm_dd",
            "futonly_or_combined",
            name="uq_cot_disagg_contract_date_type",
        ),
    )

    op.create_index(
        "ix_cot_disagg_market_name",
        "cot_disaggregated_futures_only",
        ["market_and_exchange_names"],
    )
    op.create_index(
        "ix_cot_disagg_report_date",
        "cot_disaggregated_futures_only",
        ["report_date_as_yyyy_mm_dd"],
    )
    op.create_index(
        "ix_cot_disagg_cftc_contract_market_code",
        "cot_disaggregated_futures_only",
        ["cftc_contract_market_code"],
    )
    op.create_index(
        "ix_cot_disagg_cftc_commodity_code",
        "cot_disaggregated_futures_only",
        ["cftc_commodity_code"],
    )


def downgrade():
    op.drop_index("ix_cot_disagg_cftc_commodity_code", table_name="cot_disaggregated_futures_only")
    op.drop_index("ix_cot_disagg_cftc_contract_market_code", table_name="cot_disaggregated_futures_only")
    op.drop_index("ix_cot_disagg_report_date", table_name="cot_disaggregated_futures_only")
    op.drop_index("ix_cot_disagg_market_name", table_name="cot_disaggregated_futures_only")
    op.drop_table("cot_disaggregated_futures_only")
