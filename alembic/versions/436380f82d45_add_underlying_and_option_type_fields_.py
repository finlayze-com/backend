"""add underlying and option type fields to option_detail

Revision ID: 436380f82d45
Revises: 1fd4acfd46f5
Create Date: 2026-03-21 09:44:58.399947

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '436380f82d45'
down_revision: Union[str, Sequence[str], None] = '1fd4acfd46f5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.add_column("option_detail", sa.Column("underlying_name", sa.Text(), nullable=True))
    op.add_column("option_detail", sa.Column("underlying_ticker", sa.Text(), nullable=True))
    op.add_column("option_detail", sa.Column("days_to_expiry", sa.Integer(), nullable=True))
    op.add_column("option_detail", sa.Column("option_type", sa.Text(), nullable=True))
    op.add_column("option_detail", sa.Column("is_call", sa.Boolean(), nullable=True))
    op.add_column("option_detail", sa.Column("is_put", sa.Boolean(), nullable=True))

    op.create_index(
        "ix_option_detail_underlying_ticker",
        "option_detail",
        ["underlying_ticker"],
        unique=False,
    )
    op.create_index(
        "ix_option_detail_days_to_expiry",
        "option_detail",
        ["days_to_expiry"],
        unique=False,
    )
    op.create_index(
        "ix_option_detail_option_type",
        "option_detail",
        ["option_type"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_option_detail_option_type", table_name="option_detail")
    op.drop_index("ix_option_detail_days_to_expiry", table_name="option_detail")
    op.drop_index("ix_option_detail_underlying_ticker", table_name="option_detail")

    op.drop_column("option_detail", "is_put")
    op.drop_column("option_detail", "is_call")
    op.drop_column("option_detail", "option_type")
    op.drop_column("option_detail", "days_to_expiry")
    op.drop_column("option_detail", "underlying_ticker")
    op.drop_column("option_detail", "underlying_name")
