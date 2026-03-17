"""create option_detail table

Revision ID: 1fd4acfd46f5
Revises: 8a381babbf83
Create Date: 2026-03-15 21:05:16.222477

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1fd4acfd46f5'
down_revision: Union[str, Sequence[str], None] = '8a381babbf83'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "option_detail",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),

        # from symboldetail
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("name_en", sa.Text(), nullable=True),
        sa.Column("sector", sa.Text(), nullable=True),
        sa.Column("sector_code", sa.Text(), nullable=True),
        sa.Column("subsector", sa.Text(), nullable=True),
        sa.Column("stock_ticker", sa.Text(), nullable=True),

        # from API response
        sa.Column("ins_code", sa.Text(), nullable=False, unique=True),
        sa.Column("instrument_id", sa.Text(), nullable=False, unique=True),
        sa.Column("buy_op", sa.BigInteger(), nullable=True),
        sa.Column("sell_op", sa.BigInteger(), nullable=True),
        sa.Column("contract_size", sa.BigInteger(), nullable=True),
        sa.Column("strike_price", sa.BigInteger(), nullable=True),
        sa.Column("ua_ins_code", sa.Text(), nullable=True),
        # store as real DATE
        sa.Column("begin_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("a_factor", sa.Float(), nullable=True),
        sa.Column("b_factor", sa.Float(), nullable=True),
        sa.Column("c_factor", sa.BigInteger(), nullable=True),

        # computed
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),

        # timestamps
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    op.create_index("ix_option_detail_ins_code", "option_detail", ["ins_code"], unique=True)
    op.create_index("ix_option_detail_instrument_id", "option_detail", ["instrument_id"], unique=True)
    op.create_index("ix_option_detail_ua_ins_code", "option_detail", ["ua_ins_code"], unique=False)
    op.create_index("ix_option_detail_stock_ticker", "option_detail", ["stock_ticker"], unique=False)
    op.create_index("ix_option_detail_is_active", "option_detail", ["is_active"], unique=False)


def downgrade():
    op.drop_index("ix_option_detail_is_active", table_name="option_detail")
    op.drop_index("ix_option_detail_stock_ticker", table_name="option_detail")
    op.drop_index("ix_option_detail_ua_ins_code", table_name="option_detail")
    op.drop_index("ix_option_detail_instrument_id", table_name="option_detail")
    op.drop_index("ix_option_detail_ins_code", table_name="option_detail")
    op.drop_table("option_detail")
