"""add greeks helper fields to option_greeks_daily

Revision ID: f509750bc3b0
Revises: cdce06f6bc7a
Create Date: 2026-03-22 16:52:07.212677

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f509750bc3b0'
down_revision: Union[str, Sequence[str], None] = 'cdce06f6bc7a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column(
        "option_greeks_daily",
        sa.Column("time_to_expiry_years", sa.Numeric(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("risk_free_rate", sa.Numeric(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("has_option_close", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("has_spot_close", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("is_greeks_computable", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("iv_solved", sa.Boolean(), nullable=True)
    )
    op.add_column(
        "option_greeks_daily",
        sa.Column("greeks_computed", sa.Boolean(), nullable=True)
    )


def downgrade():
    op.drop_column("option_greeks_daily", "greeks_computed")
    op.drop_column("option_greeks_daily", "iv_solved")
    op.drop_column("option_greeks_daily", "is_greeks_computable")
    op.drop_column("option_greeks_daily", "has_spot_close")
    op.drop_column("option_greeks_daily", "has_option_close")
    op.drop_column("option_greeks_daily", "risk_free_rate")
    op.drop_column("option_greeks_daily", "time_to_expiry_years")