"""create option_greeks_daily table

Revision ID: 880998aba4db
Revises: 0b3d3f459874
Create Date: 2026-03-21 13:09:00.794217

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '880998aba4db'
down_revision: Union[str, Sequence[str], None] = '0b3d3f459874'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

"""create option_greeks_daily table

Revision ID: 9f8e_option_greeks_daily
Revises: 3a40915215e2
Create Date: 2026-03-21
"""

def upgrade():
    op.create_table(
        "option_greeks_daily",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),

        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("option_ticker", sa.Text(), nullable=False),
        sa.Column("underlying_ticker", sa.Text(), nullable=True),
        sa.Column("ins_code", sa.Text(), nullable=True),
        sa.Column("option_type", sa.Text(), nullable=True),

        sa.Column("strike_price", sa.Numeric(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("days_to_expiry", sa.Integer(), nullable=True),

        sa.Column("spot_close", sa.Numeric(), nullable=True),
        sa.Column("option_close", sa.Numeric(), nullable=True),

        sa.Column("intrinsic_value", sa.Numeric(), nullable=True),
        sa.Column("extrinsic_value", sa.Numeric(), nullable=True),
        sa.Column("break_even", sa.Numeric(), nullable=True),
        sa.Column("moneyness_raw", sa.Numeric(), nullable=True),
        sa.Column("moneyness_pct", sa.Numeric(), nullable=True),

        sa.Column("iv", sa.Numeric(), nullable=True),
        sa.Column("delta", sa.Numeric(), nullable=True),
        sa.Column("gamma", sa.Numeric(), nullable=True),
        sa.Column("theta", sa.Numeric(), nullable=True),
        sa.Column("vega", sa.Numeric(), nullable=True),

        sa.Column("is_active", sa.Boolean(), nullable=True),

        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),

        sa.UniqueConstraint(
            "trade_date",
            "option_ticker",
            name="uq_option_greeks_daily_trade_date_option_ticker"
        ),
    )

    op.create_index(
        "ix_option_greeks_daily_trade_date",
        "option_greeks_daily",
        ["trade_date"],
        unique=False,
    )
    op.create_index(
        "ix_option_greeks_daily_option_ticker",
        "option_greeks_daily",
        ["option_ticker"],
        unique=False,
    )
    op.create_index(
        "ix_option_greeks_daily_underlying_ticker",
        "option_greeks_daily",
        ["underlying_ticker"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_option_greeks_daily_underlying_ticker", table_name="option_greeks_daily")
    op.drop_index("ix_option_greeks_daily_option_ticker", table_name="option_greeks_daily")
    op.drop_index("ix_option_greeks_daily_trade_date", table_name="option_greeks_daily")
    op.drop_table("option_greeks_daily")