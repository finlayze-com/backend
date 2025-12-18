"""create trade_history table

Revision ID: 4494b423ec29
Revises: bdab786491fa
Create Date: 2025-12-18 18:10:15.928659

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4494b423ec29'
down_revision: Union[str, Sequence[str], None] = 'bdab786491fa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "trade_history",

        # --- identity ---
        sa.Column("insCode", sa.BigInteger(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),

        # --- metadata from symbolDetail ---
        sa.Column("market", sa.Text(), nullable=True),            # نام فارسی بازار
        sa.Column("instrument_type", sa.Text(), nullable=True),   # نوع ابزار
        sa.Column("sector", sa.Text(), nullable=True),            # صنعت

        # --- trade data ---
        sa.Column("dEven", sa.Integer(), nullable=False),          # YYYYMMDD
        sa.Column("nTran", sa.Integer(), nullable=False),
        sa.Column("hEven", sa.Integer(), nullable=False),          # HHMMSS
        sa.Column("pTran", sa.Numeric(18, 2), nullable=False),
        sa.Column("qTitTran", sa.BigInteger(), nullable=False),
        sa.Column("canceled", sa.SmallInteger(), nullable=False),

        # --- audit ---
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False
        ),

        sa.PrimaryKeyConstraint(
            "insCode", "dEven", "nTran",
            name="pk_trade_history"
        )
    )

    # ایندکس برای سرعت
    op.create_index(
        "ix_trade_history_inscode_deven",
        "trade_history",
        ["insCode", "dEven"],
        unique=False
    )

    op.create_index(
        "ix_trade_history_sector",
        "trade_history",
        ["sector"],
        unique=False
    )


def downgrade():
    op.drop_index("ix_trade_history_sector", table_name="trade_history")
    op.drop_index("ix_trade_history_inscode_deven", table_name="trade_history")
    op.drop_table("trade_history")
