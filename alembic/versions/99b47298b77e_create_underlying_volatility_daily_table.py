"""create underlying volatility daily table

Revision ID: 99b47298b77e
Revises: f509750bc3b0
Create Date: 2026-04-26 12:19:09.999263

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '99b47298b77e'
down_revision: Union[str, Sequence[str], None] = 'f509750bc3b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        "underlying_volatility_daily",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),

        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("ticker", sa.Text(), nullable=False),
        sa.Column("asset_source", sa.Text(), nullable=True),

        sa.Column("close_price", sa.Numeric(), nullable=True),
        sa.Column("log_return", sa.Numeric(), nullable=True),

        sa.Column("hv_10d", sa.Numeric(), nullable=True),
        sa.Column("hv_20d", sa.Numeric(), nullable=True),
        sa.Column("hv_30d", sa.Numeric(), nullable=True),
        sa.Column("hv_60d", sa.Numeric(), nullable=True),

        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),

        sa.UniqueConstraint(
            "trade_date",
            "ticker",
            name="uq_underlying_volatility_daily_trade_date_ticker",
        ),
    )

    op.create_index(
        "ix_underlying_volatility_daily_trade_date",
        "underlying_volatility_daily",
        ["trade_date"],
        unique=False,
    )

    op.create_index(
        "ix_underlying_volatility_daily_ticker",
        "underlying_volatility_daily",
        ["ticker"],
        unique=False,
    )

    op.create_index(
        "ix_underlying_volatility_daily_asset_source",
        "underlying_volatility_daily",
        ["asset_source"],
        unique=False,
    )


def downgrade():
    op.drop_index(
        "ix_underlying_volatility_daily_asset_source",
        table_name="underlying_volatility_daily",
    )
    op.drop_index(
        "ix_underlying_volatility_daily_ticker",
        table_name="underlying_volatility_daily",
    )
    op.drop_index(
        "ix_underlying_volatility_daily_trade_date",
        table_name="underlying_volatility_daily",
    )
    op.drop_table("underlying_volatility_daily")