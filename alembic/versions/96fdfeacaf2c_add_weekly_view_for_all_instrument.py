"""add weekly view for all instrument

Revision ID: 96fdfeacaf2c
Revises: 5c993bfc0a57
Create Date: 2025-08-29 12:13:15.193979

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '96fdfeacaf2c'
down_revision: Union[str, Sequence[str], None] = '5c993bfc0a57'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""create weekly joined views per tool (with/without indicators)

Revision ID: <PUT_YOUR_NEW_REV_ID>
Revises: 5c1a85cd8cae
Create Date: 2025-08-29 15:30:00
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# =========================
# قالب ویوهای «با اندیکاتور»
# =========================
VIEW_SQL_WITH_INDICATORS = """
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    'W'::text AS time_frame,
    d.*,

    -- weekly indicators (ریالی)
    wi.ema_20, wi.ema_50, wi.ema_100,
    wi.rsi, wi.macd, wi.macd_signal, wi.macd_hist,
    wi.tenkan, wi.kijun, wi.senkou_a, wi.senkou_b, wi.chikou,
    wi.signal_ichimoku_buy, wi.signal_ichimoku_sell,
    wi.signal_ema_cross_buy, wi.signal_ema_cross_sell,
    wi.signal_rsi_buy, wi.signal_rsi_sell,
    wi.signal_macd_buy, wi.signal_macd_sell,
    wi.signal_ema50_100_buy, wi.signal_ema50_100_sell,
    wi.atr_52, wi.renko_52,

    -- weekly indicators (دلاری)
    wi.ema_20_d, wi.ema_50_d, wi.ema_100_d,
    wi.rsi_d, wi.macd_d, wi.macd_signal_d, wi.macd_hist_d,
    wi.tenkan_d, wi.kijun_d, wi.senkou_a_d, wi.senkou_b_d, wi.chikou_d,
    wi.signal_ichimoku_buy_d, wi.signal_ichimoku_sell_d,
    wi.signal_ema_cross_buy_d, wi.signal_ema_cross_sell_d,
    wi.signal_rsi_buy_d, wi.signal_rsi_sell_d,
    wi.signal_macd_buy_d, wi.signal_macd_sell_d,
    wi.signal_ema50_100_buy_d, wi.signal_ema50_100_sell_d,
    wi.atr_52_d, wi.renko_52_d,

    -- haghighi هفتگی (اگر دارید)
    wh.buy_i_volume, wh.buy_n_volume,
    wh.buy_i_value, wh.buy_n_value, wh.buy_n_count,
    wh.sell_i_volume, wh.buy_i_count, wh.sell_n_volume,
    wh.sell_i_value, wh.sell_n_value, wh.sell_n_count, wh.sell_i_count,

    -- symboldetail
    sd.name_en, sd.sector, sd.sector_code, sd.subsector,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

    -- متریک‌های بازار
    (d.adjust_close * sd.share_number) AS marketcap,
    (d.adjust_close * sd.share_number) / NULLIF(d.dollar_rate,0) AS marketcap_usd,

    -- هویت یکتا
    si.symbol_id
FROM {source_table} d
LEFT JOIN {indicator_table} wi
  ON d.stock_ticker = wi.stock_ticker AND d.week_end = wi.week_end
LEFT JOIN weekly_haghighi wh
  ON d.stock_ticker = wh.symbol AND d.week_end = wh.week_end
LEFT JOIN symboldetail sd
  ON d.stock_ticker = sd.stock_ticker
LEFT JOIN symbol_identity si
  ON si.stock_ticker = d.stock_ticker;
"""

# =========================
# قالب ویوهای «بدون اندیکاتور»
# =========================
VIEW_SQL_NO_INDICATORS = """
CREATE OR REPLACE VIEW {view_name} AS
SELECT
    'W'::text AS time_frame,
    d.*,

    -- haghighi هفتگی (اگر دارید)
    wh.buy_i_volume, wh.buy_n_volume,
    wh.buy_i_value, wh.buy_n_value, wh.buy_n_count,
    wh.sell_i_volume, wh.buy_i_count, wh.sell_n_volume,
    wh.sell_i_value, wh.sell_n_value, wh.sell_n_count, wh.sell_i_count,

    -- symboldetail
    sd.name_en, sd.sector, sd.sector_code, sd.subsector,
    sd.panel, sd.share_number, sd.base_vol, sd."instrumentID",

    -- متریک‌های بازار
    (d.adjust_close * sd.share_number) AS marketcap,
    (d.adjust_close * sd.share_number) / NULLIF(d.dollar_rate,0) AS marketcap_usd,

    -- هویت یکتا
    si.symbol_id
FROM {source_table} d
LEFT JOIN weekly_haghighi wh
  ON d.stock_ticker = wh.symbol AND d.week_end = wh.week_end
LEFT JOIN symboldetail sd
  ON d.stock_ticker = sd.stock_ticker
LEFT JOIN symbol_identity si
  ON si.stock_ticker = d.stock_ticker;
"""

def _drop_view(name: str):
    # فقط VIEW را پاک کن (نه materialized) تا خطای نوع نگیری
    op.execute(f'DROP VIEW IF EXISTS {name} CASCADE;')

def upgrade() -> None:
    # ابزارهایی که اندیکاتور هفتگی اختصاصی دارند
    with_inds = [
        # (view_name, source_table, indicator_table)
        ("weekly_joined_stock",             "weekly_stock_data",            "weekly_indicators"),
        ("weekly_joined_fund_gold",         "weekly_fund_gold",             "weekly_indicators_fund_gold"),
        ("weekly_joined_fund_index_stock",  "weekly_fund_index_stock",      "weekly_indicators_fund_index_stock"),
        ("weekly_joined_fund_leverage",     "weekly_fund_leverage",         "weekly_indicators_fund_leverage"),
        ("weekly_joined_fund_segment",      "weekly_fund_segment",          "weekly_indicators_fund_segment"),
    ]

    # ابزارهایی که فعلاً اندیکاتور اختصاصی ندارند (فقط داده + haghighi + symboldetail + symbol_id)
    no_inds = [
        ("weekly_joined_fund_balanced",   "weekly_fund_balanced"),
        ("weekly_joined_fund_fixincome",  "weekly_fund_fixincome"),
        ("weekly_joined_fund_stock",      "weekly_fund_stock"),
        ("weekly_joined_fund_other",      "weekly_fund_other"),
        ("weekly_joined_rights_issue",    "weekly_rights_issue"),
        ("weekly_joined_retail",          "weekly_retail"),
        ("weekly_joined_block",           "weekly_block"),
        ("weekly_joined_option",          "weekly_option"),
        ("weekly_joined_commodity",       "weekly_commodity"),
        ("weekly_joined_bond",            "weekly_bond"),
        # اگر weekly_zafran دارید اضافه کنید؛ در برخی دیتابیس‌ها ندارید:
        # ("weekly_joined_zafran",          "weekly_zafran"),
    ]

    # پاک‌سازی ایمن (اگر قبلاً ساخته شده‌اند)
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
    names = [
        "weekly_joined_stock",
        "weekly_joined_fund_gold",
        "weekly_joined_fund_index_stock",
        "weekly_joined_fund_leverage",
        "weekly_joined_fund_segment",
        "weekly_joined_fund_balanced",
        "weekly_joined_fund_fixincome",
        "weekly_joined_fund_stock",
        "weekly_joined_fund_other",
        "weekly_joined_rights_issue",
        "weekly_joined_retail",
        "weekly_joined_block",
        "weekly_joined_option",
        "weekly_joined_commodity",
        "weekly_joined_bond",
        # "weekly_joined_zafran",
    ]
    for v in names:
        _drop_view(v)
