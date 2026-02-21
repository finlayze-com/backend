"""create mv_live_sector_report

Revision ID: 4296e7cb75c4
Revises: 4494b423ec29
Create Date: 2026-02-07 16:38:21.344268

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4296e7cb75c4'
down_revision: Union[str, Sequence[str], None] = '4494b423ec29'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# -*- coding: utf-8 -*-

def upgrade():
    op.execute(r"""
    DROP MATERIALIZED VIEW IF EXISTS mv_live_sector_report;
    CREATE MATERIALIZED VIEW mv_live_sector_report AS
    WITH latest_live AS (
      SELECT max("Download") AS ts
      FROM live_market_data
    ),

    last_daily AS (
      SELECT max(date_miladi)::date AS d
      FROM daily_joined_data
    ),

    daily_close_union AS (
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_data
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_balanced
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_fixincome
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_gold
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_index_stock
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_leverage
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_other
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_segment
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_stock
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)

      UNION ALL
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        close::numeric AS close,
        date_miladi::date AS d
      FROM daily_joined_fund_zafran
      WHERE close IS NOT NULL AND close <> 0
        AND date_miladi::date <= (SELECT d FROM last_daily)
    ),

    daily_last_close AS (
      SELECT DISTINCT ON (ticker_key)
        ticker_key,
        close AS prev_close
      FROM daily_close_union
      ORDER BY ticker_key, d DESC
    ),

    -- ✅ اینجا subsector خالی را به other تبدیل نمی‌کنیم
    sym_etf_raw AS (
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower("stock_ticker")), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        NULLIF(trim("subsector"), '') AS subsector_raw,
        NULLIF(trim("instrument_type"), '') AS instrument_type
      FROM public.symboldetail
      WHERE "sector" = 'صندوق سرمايه گذاري قابل معامله'
        AND "market" <> 'بازار مشتقه'
    ),

    sym_etf_norm AS (
      SELECT
        ticker_key,
        instrument_type,
        -- اگر subsector خالی بود، یک رشته خالی می‌گذاریم تا نرمال‌سازی خراب نشود
        regexp_replace(
          regexp_replace(
            replace(replace(replace(trim(lower(COALESCE(subsector_raw, ''))), 'ي','ی'),'ك','ک'), chr(8204), ''),
            '\s*:\s*', ' : ', 'g'
          ),
          '\s+', ' ', 'g'
        ) AS subsector_clean
      FROM sym_etf_raw
    ),

    sym_etf AS (
      SELECT
        ticker_key,
        CASE
          -- ✅ شرط‌های instrument_type وقتی subsector خالی است
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_gold'
            THEN 'طلا'
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_zafran'
            THEN 'زعفران'

          -- ✅ املاک و مستغلات (متن دقیق شما)
          WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%'
            THEN 'املاک و مستغلات'

          -- ✅ سهامی شاخصی باید قبل از سهامی بیاید تا داخل سهامی نیفتد
          WHEN subsector_clean ILIKE '%سهام%' AND subsector_clean ILIKE '%شاخص%'
            THEN 'سهامی شاخصی'
          WHEN subsector_clean ILIKE '%سهامي%' AND subsector_clean ILIKE '%شاخصي%'
            THEN 'سهامی شاخصی'

          WHEN subsector_clean ILIKE '%اهرم%' THEN 'اهرمـی'
          WHEN subsector_clean ILIKE '%طلا%' OR subsector_clean ILIKE '%سکه%' THEN 'طلا'

          WHEN subsector_clean ILIKE '%بخشی%' THEN 'بخشی'

          WHEN subsector_clean ILIKE '%درآمد ثابت%'
            OR subsector_clean ILIKE '%در امد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوارق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با%درآمد ثابت%'
          THEN 'درآمد ثابت'

          WHEN subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامی%' OR subsector_clean ILIKE '%سهامي%'
            THEN 'سهامی'
          WHEN subsector_clean ILIKE '%مختلط%' THEN 'مختلط'
          WHEN subsector_clean ILIKE '%کالا%' OR subsector_clean ILIKE '%commodity%' THEN 'کالایی'
          ELSE 'other'
        END AS subsector_norm
      FROM sym_etf_norm
      GROUP BY 1, 2
    ),

    base AS (
      SELECT
        l."Download" AS ts,
        l."Ticker"   AS stock_ticker,
        COALESCE(NULLIF(trim(l."Sector"), ''), 'unknown') AS sector,

        COALESCE(l."Value",  0)::numeric  AS value,
        COALESCE(l."Volume", 0)::numeric  AS volume,
        COALESCE(l."Final", l."Close")::numeric AS last_price,

        COALESCE(l."Vol_Buy_R",  0)::numeric AS vol_buy_r,
        COALESCE(l."Vol_Sell_R", 0)::numeric AS vol_sell_r,
        COALESCE(l."Vol_Buy_I",  0)::numeric AS vol_buy_i,
        COALESCE(l."Vol_Sell_I", 0)::numeric AS vol_sell_i,

        regexp_replace(
          replace(replace(replace(trim(lower(l."Ticker")), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key

      FROM live_market_data l
      JOIN latest_live x ON l."Download" = x.ts
      WHERE l."Ticker" !~ '[24]'
    ),

    base2 AS (
      SELECT
        b.*,
        d.prev_close,
        CASE
          WHEN b.sector = 'صندوق سرمایه گذاری قابل معامله'
            THEN COALESCE(se.subsector_norm, 'other')
          ELSE NULL
        END AS etf_subsector
      FROM base b
      LEFT JOIN daily_last_close d
        ON d.ticker_key = b.ticker_key
      LEFT JOIN sym_etf se
        ON se.ticker_key = b.ticker_key
    ),

    sector_rows AS (
      SELECT
        ts,
        'sector'::text AS level,

        CASE
          WHEN sector = 'صندوق سرمایه گذاری قابل معامله'
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(etf_subsector, 'other')
          ELSE sector
        END AS key,

        1 AS sort_order,

        COUNT(*) AS symbols_count,
        SUM(value)  AS total_value,
        SUM(volume) AS total_volume,

        AVG(
          CASE
            WHEN prev_close IS NULL OR prev_close = 0 OR last_price IS NULL THEN NULL
            WHEN last_price > prev_close THEN 1 ELSE 0
          END
        ) AS green_ratio,

        AVG(
          CASE
            WHEN prev_close IS NULL OR prev_close = 0 OR last_price IS NULL THEN NULL
            ELSE 100.0 * (last_price - prev_close) / prev_close
          END
        ) AS eqw_avg_ret_pct,

        SUM((vol_buy_r - vol_sell_r) * last_price) AS net_real_value,
        SUM((vol_buy_i - vol_sell_i) * last_price) AS net_legal_value

      FROM base2
      GROUP BY
        ts,
        CASE
          WHEN sector = 'صندوق سرمایه گذاری قابل معامله'
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(etf_subsector, 'other')
          ELSE sector
        END
    ),

    market_row AS (
      SELECT
        ts,
        'market'::text AS level,
        '__ALL__'::text AS key,
        0 AS sort_order,

        COUNT(*) AS symbols_count,
        SUM(value)  AS total_value,
        SUM(volume) AS total_volume,

        AVG(
          CASE
            WHEN prev_close IS NULL OR prev_close = 0 OR last_price IS NULL THEN NULL
            WHEN last_price > prev_close THEN 1 ELSE 0
          END
        ) AS green_ratio,

        AVG(
          CASE
            WHEN prev_close IS NULL OR prev_close = 0 OR last_price IS NULL THEN NULL
            ELSE 100.0 * (last_price - prev_close) / prev_close
          END
        ) AS eqw_avg_ret_pct,

        SUM((vol_buy_r - vol_sell_r) * last_price) AS net_real_value,
        SUM((vol_buy_i - vol_sell_i) * last_price) AS net_legal_value

      FROM base2
      GROUP BY ts
    ),

    unioned AS (
      SELECT * FROM market_row
      UNION ALL
      SELECT * FROM sector_rows
    )

    SELECT
      ts,
      level,
      key,
      sort_order,
      symbols_count,
      total_value,
      total_volume,
      green_ratio,
      eqw_avg_ret_pct,
      net_real_value,
      net_legal_value
    FROM unioned;
    """)

    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_live_sector_report_ts_level_key
      ON mv_live_sector_report (ts, level, key);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_live_sector_report_ts
      ON mv_live_sector_report (ts DESC);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_live_sector_report_level
      ON mv_live_sector_report (level);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_live_sector_report_key
      ON mv_live_sector_report (key);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_live_sector_report_total_value
      ON mv_live_sector_report (total_value DESC);
    """)


def downgrade():
    op.execute("""DROP MATERIALIZED VIEW IF EXISTS mv_live_sector_report;""")