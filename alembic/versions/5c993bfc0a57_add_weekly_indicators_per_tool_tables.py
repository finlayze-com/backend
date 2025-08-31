"""add weekly indicators per tool tables

Revision ID: 5c993bfc0a57
Revises: 5c1a85cd8cae
Create Date: 2025-08-29 11:50:23.641927

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5c993bfc0a57'
down_revision: Union[str, Sequence[str], None] = '5c1a85cd8cae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

"""add weekly indicators per tool tables

Revision ID: REPLACE_ME_XXXX
Revises: <PUT_PREVIOUS_REVISION_ID_HERE>
Create Date: 2025-08-29 00:00:00.000000
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


def _indicator_columns():
    """ستون‌های مشترک اندیکاتورهای هفتگی؛ دقیقاً مطابق weekly_indicators شما."""
    return [
        sa.Column("stock_ticker", sa.Text(), nullable=False),
        sa.Column("week_end", sa.Date(), nullable=False),

        sa.Column("ema_20", sa.Float(), nullable=True),
        sa.Column("ema_50", sa.Float(), nullable=True),
        sa.Column("ema_100", sa.Float(), nullable=True),
        sa.Column("rsi", sa.Float(), nullable=True),
        sa.Column("macd", sa.Float(), nullable=True),
        sa.Column("macd_signal", sa.Float(), nullable=True),
        sa.Column("macd_hist", sa.Float(), nullable=True),
        sa.Column("tenkan", sa.Float(), nullable=True),
        sa.Column("kijun", sa.Float(), nullable=True),
        sa.Column("senkou_a", sa.Float(), nullable=True),
        sa.Column("senkou_b", sa.Float(), nullable=True),
        sa.Column("chikou", sa.Float(), nullable=True),

        sa.Column("signal_ichimoku_buy", sa.SmallInteger(), nullable=True),
        sa.Column("signal_ichimoku_sell", sa.SmallInteger(), nullable=True),
        sa.Column("signal_ema_cross_buy", sa.SmallInteger(), nullable=True),
        sa.Column("signal_ema_cross_sell", sa.SmallInteger(), nullable=True),
        sa.Column("signal_rsi_buy", sa.SmallInteger(), nullable=True),
        sa.Column("signal_rsi_sell", sa.SmallInteger(), nullable=True),
        sa.Column("signal_macd_buy", sa.SmallInteger(), nullable=True),
        sa.Column("signal_macd_sell", sa.SmallInteger(), nullable=True),
        sa.Column("signal_ema50_100_buy", sa.SmallInteger(), nullable=True),
        sa.Column("signal_ema50_100_sell", sa.SmallInteger(), nullable=True),

        sa.Column("atr_52", sa.Numeric(), nullable=True),
        sa.Column("renko_52", sa.Text(), nullable=True),

        # نسخه‌ی دلاری (_d) — طبق تایپ‌هایی که دادی (اغلب numeric/int)
        sa.Column("ema_20_d", sa.Numeric(), nullable=True),
        sa.Column("ema_50_d", sa.Numeric(), nullable=True),
        sa.Column("ema_100_d", sa.Numeric(), nullable=True),
        sa.Column("rsi_d", sa.Numeric(), nullable=True),
        sa.Column("macd_d", sa.Numeric(), nullable=True),
        sa.Column("macd_signal_d", sa.Numeric(), nullable=True),
        sa.Column("macd_hist_d", sa.Numeric(), nullable=True),
        sa.Column("tenkan_d", sa.Numeric(), nullable=True),
        sa.Column("kijun_d", sa.Numeric(), nullable=True),
        sa.Column("senkou_a_d", sa.Numeric(), nullable=True),
        sa.Column("senkou_b_d", sa.Numeric(), nullable=True),
        sa.Column("chikou_d", sa.Numeric(), nullable=True),

        sa.Column("signal_ichimoku_buy_d", sa.Integer(), nullable=True),
        sa.Column("signal_ichimoku_sell_d", sa.Integer(), nullable=True),
        sa.Column("signal_ema_cross_buy_d", sa.Integer(), nullable=True),
        sa.Column("signal_ema_cross_sell_d", sa.Integer(), nullable=True),
        sa.Column("signal_rsi_buy_d", sa.Integer(), nullable=True),
        sa.Column("signal_rsi_sell_d", sa.Integer(), nullable=True),
        sa.Column("signal_macd_buy_d", sa.Integer(), nullable=True),
        sa.Column("signal_macd_sell_d", sa.Integer(), nullable=True),
        sa.Column("signal_ema50_100_buy_d", sa.Integer(), nullable=True),
        sa.Column("signal_ema50_100_sell_d", sa.Integer(), nullable=True),

        sa.Column("atr_52_d", sa.Numeric(), nullable=True),
        sa.Column("renko_52_d", sa.Text(), nullable=True),
    ]


def _create_indicator_table(table_name: str):
    """بساز جدول اندیکاتور هفتگی با PK مرکب و ایندکس کمکی روی تیکر."""
    op.create_table(
        table_name,
        *_indicator_columns(),
        sa.PrimaryKeyConstraint(
            "stock_ticker", "week_end",
            name=f"pk_{table_name}_ticker_week_end",
        ),
    )
    # ایندکس کمکی روی تیکر برای کوئری‌های per-symbol
    op.create_index(
        f"ix_{table_name}_stock_ticker",
        table_name,
        ["stock_ticker"],
        unique=False,
    )


def upgrade() -> None:
    # چهار جدولِ per-tool
    _create_indicator_table("weekly_indicators_fund_gold")
    _create_indicator_table("weekly_indicators_fund_index_stock")
    _create_indicator_table("weekly_indicators_fund_leverage")
    _create_indicator_table("weekly_indicators_fund_segment")


def downgrade() -> None:
    # برعکسِ upgrade: اول ایندکس‌ها، بعد جدول‌ها
    for tbl in [
        "weekly_indicators_fund_segment",
        "weekly_indicators_fund_leverage",
        "weekly_indicators_fund_index_stock",
        "weekly_indicators_fund_gold",
    ]:
        op.drop_index(f"ix_{tbl}_stock_ticker", table_name=tbl)
        op.drop_table(tbl)
