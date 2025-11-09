"""create daily_joined_data view with symbol_id

Revision ID: a8e36f6f4b01
Revises: b09a69bcc94e
Create Date: 2025-08-27 10:05:15.328496
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa  # noqa

# revision identifiers, used by Alembic.
revision: str = "a8e36f6f4b01"
down_revision: Union[str, Sequence[str], None] = "b09a69bcc94e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # حذف امن daily_joined_data (اگر view یا materialized view باشد)
    op.execute("""
       DO $do$
       BEGIN
           -- drop if TABLE exists
           IF EXISTS (
               SELECT 1 FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname='public' AND c.relname='daily_joined_data' AND c.relkind='r'
           ) THEN
               EXECUTE 'DROP TABLE IF EXISTS public.daily_joined_data CASCADE';
           END IF;

           -- drop if MATERIALIZED VIEW exists
           IF EXISTS (
               SELECT 1 FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname='public' AND c.relname='daily_joined_data' AND c.relkind='m'
           ) THEN
               EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS public.daily_joined_data CASCADE';
           END IF;

           -- drop if VIEW exists
           IF EXISTS (
               SELECT 1 FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE n.nspname='public' AND c.relname='daily_joined_data' AND c.relkind='v'
           ) THEN
               EXECUTE 'DROP VIEW IF EXISTS public.daily_joined_data CASCADE';
           END IF;
       END
       $do$;
       """)

    # ساخت ویو جدید
    op.execute("""
        CREATE OR REPLACE VIEW public.daily_joined_data AS
        SELECT
            -- 🟢 تمام ستون‌های روزانه سهم
            dsd.*,

            -- 🔵 اندیکاتورها
            di.ema_20, di.ema_50, di.ema_100,
            di.rsi, di.macd, di.macd_signal, di.macd_hist,
            di.tenkan, di.kijun, di.senkou_a, di.senkou_b, di.chikou,
            di.signal_ichimoku_buy, di.signal_ichimoku_sell,
            di.signal_ema_cross_buy, di.signal_ema_cross_sell,
            di.signal_rsi_buy, di.signal_rsi_sell,
            di.signal_macd_buy, di.signal_macd_sell,
            di.signal_ema50_100_buy, di.signal_ema50_100_sell,
            di.atr_22, di.renko_22,

            -- 💵 نسخه دلاری سیگنال‌ها
            di.tenkan_d,
            di.kijun_d,
            di.senkou_a_d,
            di.senkou_b_d,
            di.signal_ichimoku_buy_d   AS signal_ichimoku_buy_usd,
            di.signal_ichimoku_sell_d  AS signal_ichimoku_sell_usd,
            di.signal_ema_cross_buy_d  AS signal_ema_cross_buy_usd,
            di.signal_ema_cross_sell_d AS signal_ema_cross_sell_usd,
            di.signal_rsi_buy_d        AS signal_rsi_buy_usd,
            di.signal_rsi_sell_d       AS signal_rsi_sell_usd,
            di.signal_macd_buy_d       AS signal_macd_buy_usd,
            di.signal_macd_sell_d      AS signal_macd_sell_usd,
            di.signal_ema50_100_buy_d  AS signal_ema50_100_buy_usd,
            di.signal_ema50_100_sell_d AS signal_ema50_100_sell_usd,
            di.renko_22_d              AS renko_22_usd,

            -- 🟠 حقیقی/حقوقی (نام ستون‌ها lowercase)
            h.inscode,
            h.buy_i_volume, h.buy_n_volume, h.buy_i_value, h.buy_n_value,
            h.buy_n_count, h.sell_i_volume, h.buy_i_count, h.sell_n_volume,
            h.sell_i_value, h.sell_n_value, h.sell_n_count, h.sell_i_count,
            h.buy_i_value_usd, h.buy_n_value_usd, h.sell_i_value_usd, h.sell_n_value_usd,

            -- 🟣 جزئیات نماد (instrumentid به‌صورت lowercase)
            sd.name_en, sd.sector, sd.sector_code, sd.subsector, sd.market AS market2,
            sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

            -- 💰 محاسبات بازار
            (dsd.adjust_close * sd.share_number) AS marketcap,
            (dsd.adjust_close * sd.share_number) / dsd.dollar_rate AS marketcap_usd,

            -- 🧩 شناسه یکتا برای گزارش‌گیری
            si.symbol_id
        FROM daily_stock_data dsd
        LEFT JOIN daily_indicators di
            ON dsd.stock_ticker = di.stock_ticker
           AND dsd.date_miladi  = di.date_miladi
        LEFT JOIN haghighi h
            ON dsd.stock_ticker = h.symbol
           AND dsd.date_miladi  = h.recdate
        LEFT JOIN symboldetail sd
            ON dsd.stock_ticker = sd.stock_ticker
        LEFT JOIN symbol_identity si
            ON si.stock_ticker = dsd.stock_ticker
    """)


def downgrade() -> None:
    op.execute("""
    DO $do$
    BEGIN
        -- اگر MATERIALIZED VIEW است
        IF EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = current_schema()
              AND matviewname = 'daily_joined_data'
        ) THEN
            EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || quote_ident(current_schema()) || '.daily_joined_data CASCADE';
        END IF;

        -- اگر VIEW است
        IF EXISTS (
            SELECT 1 FROM pg_views
            WHERE schemaname = current_schema()
              AND viewname = 'daily_joined_data'
        ) THEN
            EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(current_schema()) || '.daily_joined_data CASCADE';
        END IF;

        -- اگر TABLE است (برای پاکسازی مطمئن)
        IF EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname  = 'daily_joined_data'
              AND c.relkind  = 'r'
        ) THEN
            EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(current_schema()) || '.daily_joined_data CASCADE';
        END IF;
    END
    $do$;
    """)
