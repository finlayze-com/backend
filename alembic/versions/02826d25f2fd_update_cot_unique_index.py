"""update cot unique index

Revision ID: 02826d25f2fd
Revises: 4491acd097f7
Create Date: 2026-05-18 10:10:25.345805

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '02826d25f2fd'
down_revision: Union[str, Sequence[str], None] = '4491acd097f7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
        DROP INDEX IF EXISTS uq_cot_disagg_hist;
    """)

    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cot_disagg_hist
        ON cot_disaggregated_futures_only (
            cftc_contract_market_code,
            cftc_market_code,
            cftc_commodity_code,
            as_of_date_in_form_yymmdd,
            futonly_or_combined
        );
    """)


def downgrade():
    op.execute("""
        DROP INDEX IF EXISTS uq_cot_disagg_hist;
    """)

    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cot_disagg_hist
        ON cot_disaggregated_futures_only (
            cftc_contract_market_code,
            as_of_date_in_form_yymmdd,
            futonly_or_combined
        );
    """)