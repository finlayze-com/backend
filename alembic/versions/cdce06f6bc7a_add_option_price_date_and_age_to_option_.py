"""add option price date and age to option_greeks_daily

Revision ID: cdce06f6bc7a
Revises: 880998aba4db
Create Date: 2026-03-22 14:42:29.193186

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cdce06f6bc7a'
down_revision: Union[str, Sequence[str], None] = '880998aba4db'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

"""add option price date and age to option_greeks_daily

Revision ID: add_option_price_date_age_to_option_greeks_daily
Revises: 9f8e_option_greeks_daily
Create Date: 2026-03-21
"""

def upgrade():
    op.add_column(
        "option_greeks_daily",
        sa.Column("option_price_date", sa.Date(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("option_price_age_days", sa.Integer(), nullable=True)
    )

    op.create_index(
        "ix_option_greeks_daily_option_price_date",
        "option_greeks_daily",
        ["option_price_date"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_option_greeks_daily_option_price_date", table_name="option_greeks_daily")
    op.drop_column("option_greeks_daily", "option_price_age_days")
    op.drop_column("option_greeks_daily", "option_price_date")