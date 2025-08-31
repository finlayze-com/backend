"""create_other_instrument_joined_data

Revision ID: 5c1a85cd8cae
Revises: bcf9500c8618
Create Date: 2025-08-29 09:07:53.308641

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5c1a85cd8cae'
down_revision: Union[str, Sequence[str], None] = 'bcf9500c8618'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""create daily joined views per tool (with/without indicators)

Revision ID: 7f0c1d2e3a45
Revises: 9a8b824a1394
Create Date: 2025-08-29 12:34:00.000000
"""

# ---------------------------
# قالب SQL با اندیکاتورها
# ---------------------------
VIEW_SQL_WITH_INDICATORS = """
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    'D'::text AS time_frame,
    d.*,

    -- indicators (ریالی)
    di.ema_20, di.ema_50, di.ema_100,
    di.rsi, di.macd, di.macd_signal, di.macd_hist,
    di.tenkan, di.kijun, di.senkou_a, di.senkou_b, di.chikou,
    di.signal_ichimoku_buy, di.signal_ichimoku_sell,
    di.signal_ema_cross_buy, di.signal_ema_cross_sell,
    di.signal_rsi_buy, di.signal_rsi_sell,
    di.signal_macd_buy, di.signal_macd_sell,
    di.signal_ema50_100_buy, di.signal_ema50_100_sell,
    di.atr_22, di.renko_22,

    -- indicators (دلاری) با نام‌های *_usd
    di.ema_20_d,  di.ema_50_d,  di.ema_100_d,
    di.rsi_d,     di.macd_d,    di.macd_signal_d, di.macd_hist_d,
    di.tenkan_d,  di.kijun_d,   di.senkou_a_d,    di.senkou_b_d,   di.chikou_d,
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
    di.atr_22_d,
    di.renko_22_d              AS renko_22_usd,

    -- haghighi (lowercase)
    h.inscode,
    h.buy_i_volume, h.buy_n_volume, h.buy_i_value, h.buy_n_value,
    h.buy_n_count, h.sell_i_volume, h.buy_i_count, h.sell_n_volume,
    h.sell_i_value, h.sell_n_value, h.sell_n_count, h.sell_i_count,
    h.buy_i_value_usd, h.buy_n_value_usd, h.sell_i_value_usd, h.sell_n_value_usd,

    -- symboldetail
    sd.name_en, sd.sector, sd.sector_code, sd.subsector, sd.market AS market2,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

    -- متریک‌های بازار
    (d.adjust_close * sd.share_number) AS marketcap,
    (d.adjust_close * sd.share_number) / NULLIF(d.dollar_rate,0) AS marketcap_usd,

    -- هویت یکتا
    si.symbol_id
FROM {source_table} d
LEFT JOIN {indicator_table} di
    ON d.stock_ticker = di.stock_ticker
   AND d.date_miladi  = di.date_miladi
LEFT JOIN haghighi h
    ON d.stock_ticker = h.symbol
   AND d.date_miladi  = h.recdate
LEFT JOIN symboldetail sd
    ON d.stock_ticker = sd.stock_ticker
LEFT JOIN symbol_identity si
    ON si.stock_ticker = d.stock_ticker;
"""

# ---------------------------
# قالب SQL بدون اندیکاتورها
# ---------------------------
VIEW_SQL_NO_INDICATORS = """
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    'D'::text AS time_frame,
    d.*,

    -- haghighi (lowercase)
    h.inscode,
    h.buy_i_volume, h.buy_n_volume, h.buy_i_value, h.buy_n_value,
    h.buy_n_count, h.sell_i_volume, h.buy_i_count, h.sell_n_volume,
    h.sell_i_value, h.sell_n_value, h.sell_n_count, h.sell_i_count,
    h.buy_i_value_usd, h.buy_n_value_usd, h.sell_i_value_usd, h.sell_n_value_usd,

    -- symboldetail
    sd.name_en, sd.sector, sd.sector_code, sd.subsector, sd.market AS market2,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

    -- متریک‌های بازار
    (d.adjust_close * sd.share_number) AS marketcap,
    (d.adjust_close * sd.share_number) / NULLIF(d.dollar_rate,0) AS marketcap_usd,

    -- هویت یکتا
    si.symbol_id
FROM {source_table} d
LEFT JOIN haghighi h
    ON d.stock_ticker = h.symbol
   AND d.date_miladi  = h.recdate
LEFT JOIN symboldetail sd
    ON d.stock_ticker = sd.stock_ticker
LEFT JOIN symbol_identity si
    ON si.stock_ticker = d.stock_ticker;
"""


def _drop_view(name: str):
    # هر نوع شیء را امن پاک می‌کند: materialized view / view / table
    op.execute(f"""
    DO $$
    DECLARE
        o    regclass := to_regclass('public.{name}');
        k    "char";
    BEGIN
        IF o IS NULL THEN
            RETURN;
        END IF;

        SELECT c.relkind INTO k
        FROM pg_class c
        WHERE c.oid = o;

        -- 'm' = materialized view, 'v' = view, 'r' = table, 'p' = partitioned table
        IF k = 'm' THEN
            EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS public.{name} CASCADE';
        ELSIF k = 'v' THEN
            EXECUTE 'DROP VIEW IF EXISTS public.{name} CASCADE';
        ELSIF k IN ('r','p') THEN
            EXECUTE 'DROP TABLE IF EXISTS public.{name} CASCADE';
        ELSE
            -- برای انواع دیگر، حداقل تلاش کن
            EXECUTE 'DROP VIEW IF EXISTS public.{name} CASCADE';
            EXECUTE 'DROP TABLE IF EXISTS public.{name} CASCADE';
        END IF;
    END
    $$;
    """)



def upgrade() -> None:
    # لیست ابزارها با اندیکاتور
    with_inds = [
        # (view_name, source_table, indicator_table)
        ("daily_joined_fund_gold",    "daily_fund_gold",         "daily_indicators_fund_gold"),
        ("daily_joined_fund_leverage","daily_fund_leverage",     "daily_indicators_fund_leverage"),
        ("daily_joined_fund_index_stock","daily_fund_index_stock","daily_indicators_fund_index_stock"),
        ("daily_joined_fund_segment", "daily_fund_segment",      "daily_indicators_fund_segment"),
    ]

    # لیست ابزارها بدون اندیکاتور اختصاصی
    no_inds = [
        ("daily_joined_fund_balanced",  "daily_fund_balanced"),
        ("daily_joined_fund_fixincome", "daily_fund_fixincome"),
        ("daily_joined_fund_stock",     "daily_fund_stock"),
        ("daily_joined_fund_other",     "daily_fund_other"),
        ("daily_joined_rights_issue",   "daily_rights_issue"),
        ("daily_joined_retail",         "daily_retail"),
        ("daily_joined_block",          "daily_block"),
        ("daily_joined_option",         "daily_option"),
        ("daily_joined_commodity",      "daily_commodity"),
        ("daily_joined_bond",           "daily_bond"),
        ("daily_joined_fund_zafran",         "daily_fund_zafran"),
    ]

    # اول پاک‌سازی
    for v, *_ in with_inds:
        _drop_view(v)
    for v, *_ in no_inds:
        _drop_view(v)

    # ساخت ویوهای با اندیکاتور
    for view_name, source_table, indicator_table in with_inds:
        op.execute(VIEW_SQL_WITH_INDICATORS.format(
            view_name=view_name,
            source_table=source_table,
            indicator_table=indicator_table,
        ))

    # ساخت ویوهای بدون اندیکاتور
    for view_name, source_table in no_inds:
        op.execute(VIEW_SQL_NO_INDICATORS.format(
            view_name=view_name,
            source_table=source_table,
        ))


def downgrade() -> None:
    # حذف همه ویوها
    with_inds = [
        "daily_joined_fund_gold",
        "daily_joined_fund_leverage",
        "daily_joined_fund_index_stock",
        "daily_joined_fund_segment",
    ]
    no_inds = [
        "daily_joined_fund_balanced",
        "daily_joined_fund_fixincome",
        "daily_joined_fund_stock",
        "daily_joined_fund_other",
        "daily_joined_rights_issue",
        "daily_joined_retail",
        "daily_joined_block",
        "daily_joined_option",
        "daily_joined_commodity",
        "daily_joined_bond",
        "daily_joined_fund_zafran",
    ]
    for v in with_inds + no_inds:
        _drop_view(v)
