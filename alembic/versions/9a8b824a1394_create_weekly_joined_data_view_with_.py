"""create weekly_joined_data view with symbol_id

Revision ID: 9a8b824a1394
Revises: 4c1b963e63c7
Create Date: 2025-08-27 12:06:53.169351

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9a8b824a1394'
down_revision: Union[str, Sequence[str], None] = '4c1b963e63c7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""create weekly_joined_data view with symbol_id

Revision ID: <Alembic will fill>
Revises: a8e36f6f4b01
Create Date: <Alembic will fill>
"""

def upgrade():
    # حذف ایمن هر نوع آبجکت با این نام (جدول/ویو/متریال)
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = current_schema() AND tablename = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP TABLE weekly_joined_data CASCADE';
        ELSIF EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = current_schema() AND matviewname = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP MATERIALIZED VIEW weekly_joined_data CASCADE';
        ELSIF EXISTS (
            SELECT 1 FROM pg_views
            WHERE schemaname = current_schema() AND viewname = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP VIEW weekly_joined_data CASCADE';
        END IF;
    END$$;
    """)

    # ویو کامل با تمام ستون‌ها + symbol_id
    op.execute("""
        CREATE OR REPLACE VIEW weekly_joined_data AS
        SELECT
            -- weekly_stock_data (ستون‌های اصلی)
            wsd.id,
            wsd.stock_ticker,
            wsd.week_start,
            wsd.week_end,
            wsd.open,
            wsd.high,
            wsd.low,
            wsd.close,
            wsd.final_price,
            wsd.adjust_open,
            wsd.adjust_high,
            wsd.adjust_low,
            wsd.adjust_close,
            wsd.adjust_final_price,
            wsd.volume,
            wsd.value,
            wsd.name,
            wsd.market,
            wsd.adjust_open_usd,
            wsd.adjust_high_usd,
            wsd.adjust_low_usd,
            wsd.adjust_close_usd,
            wsd.value_usd,
            wsd.dollar_rate,

            -- weekly_indicators
            wi.ema_20,
            wi.ema_50,
            wi.ema_100,
            wi.rsi,
            wi.macd,
            wi.macd_signal,
            wi.macd_hist,
            wi.tenkan,
            wi.kijun,
            wi.senkou_a,
            wi.senkou_b,
            wi.chikou,
            wi.signal_ichimoku_buy,
            wi.signal_ichimoku_sell,
            wi.signal_ema_cross_buy,
            wi.signal_ema_cross_sell,
            wi.signal_rsi_buy,
            wi.signal_rsi_sell,
            wi.signal_macd_buy,
            wi.signal_macd_sell,
            wi.signal_ema50_100_buy,
            wi.signal_ema50_100_sell,
            wi.atr_52,
            wi.renko_52,

            wi.ema_20_d,
            wi.ema_50_d,
            wi.ema_100_d,
            wi.rsi_d,
            wi.macd_d,
            wi.macd_signal_d,
            wi.macd_hist_d,
            wi.tenkan_d,
            wi.kijun_d,
            wi.senkou_a_d,
            wi.senkou_b_d,
            wi.chikou_d,
            wi.signal_ichimoku_buy_d,
            wi.signal_ichimoku_sell_d,
            wi.signal_ema_cross_buy_d,
            wi.signal_ema_cross_sell_d,
            wi.signal_rsi_buy_d,
            wi.signal_rsi_sell_d,
            wi.signal_macd_buy_d,
            wi.signal_macd_sell_d,
            wi.signal_ema50_100_buy_d,
            wi.signal_ema50_100_sell_d,
            wi.atr_52_d,
            wi.renko_52_d,

            -- weekly_haghighi (تجمیع حقیقی/حقوقی هفتگی)
            wh.buy_i_volume,
            wh.buy_n_volume,
            wh.buy_i_value,
            wh.buy_n_value,
            wh.buy_n_count,
            wh.sell_i_volume,
            wh.buy_i_count,
            wh.sell_n_volume,
            wh.sell_i_value,
            wh.sell_n_value,
            wh.sell_n_count,
            wh.sell_i_count,

            -- symboldetail (متادیتای نماد)
            sd.name_en,
            sd.sector,
            sd.sector_code,
            sd.subsector,
            sd.panel,
            sd.share_number,
            sd.base_vol,
            sd."instrumentID",

            -- محاسبات بازار
            (wsd.adjust_close * sd.share_number) AS marketcap,
            (wsd.adjust_close * sd.share_number) / NULLIF(wsd.dollar_rate, 0) AS marketcap_usd,

            -- شناسه یکتا
            si.symbol_id

        FROM weekly_stock_data wsd
        LEFT JOIN weekly_indicators wi
            ON wsd.stock_ticker = wi.stock_ticker
           AND wsd.week_end     = wi.week_end
        LEFT JOIN weekly_haghighi wh
            ON wsd.stock_ticker = wh.symbol
           AND wsd.week_end     = wh.week_end
        LEFT JOIN symboldetail sd
            ON wsd.stock_ticker = sd.stock_ticker
        LEFT JOIN symbol_identity si
            ON si.stock_ticker  = wsd.stock_ticker
        ;
    """)


def downgrade():
    op.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_views
            WHERE schemaname = current_schema() AND viewname = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP VIEW weekly_joined_data CASCADE';
        ELSIF EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = current_schema() AND matviewname = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP MATERIALIZED VIEW weekly_joined_data CASCADE';
        ELSIF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = current_schema() AND tablename = 'weekly_joined_data'
        ) THEN
            EXECUTE 'DROP TABLE weekly_joined_data CASCADE';
        END IF;
    END$$;
    """)