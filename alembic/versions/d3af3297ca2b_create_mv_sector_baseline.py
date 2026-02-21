"""create mv_sector_baseline

Revision ID: d3af3297ca2b
Revises: 2e0542b2db24
Create Date: 2026-02-08 15:51:56.610569
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'd3af3297ca2b'
down_revision: Union[str, Sequence[str], None] = '2e0542b2db24'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

MV_NAME = "mv_sector_baseline"


def upgrade():
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV_NAME};")

    op.execute(rf"""
    CREATE MATERIALIZED VIEW {MV_NAME} AS
    WITH
    /* ----------------------------
      0) ETF classification (same logic)
    -----------------------------*/
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
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_gold'
            THEN 'طلا'
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
               AND instrument_type = 'fund_zafran'
            THEN 'زعفران'

          WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%'
            THEN 'املاک و مستغلات'

          -- سهامی شاخصی قبل از سهامی
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

          WHEN subsector_clean ILIKE '%مختلط%' THEN 'مختلط'
          WHEN subsector_clean ILIKE '%کالا%' OR subsector_clean ILIKE '%commodity%' THEN 'کالایی'

          WHEN subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامی%' OR subsector_clean ILIKE '%سهامي%'
            THEN 'سهامی'

          ELSE 'other'
        END AS etf_bucket
      FROM sym_etf_norm
      GROUP BY 1,2
    ),

    /* ----------------------------
      1) daily UNION (stocks + all funds)
      نکته: ETFها در daily_joined_fund_* هستند، پس باید union شوند.
    -----------------------------*/
    daily_union AS (
      SELECT date_miladi::date AS d, stock_ticker, sector, value, marketcap
      FROM daily_joined_data

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_balanced

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_fixincome

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_gold

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_index_stock

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_leverage

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_other

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_segment

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_stock

      UNION ALL
      SELECT date_miladi::date, stock_ticker, sector, value, marketcap
      FROM daily_joined_fund_zafran
    ),

    daily0 AS (
      SELECT
        u.d,
        COALESCE(NULLIF(trim(u.sector), ''), 'other') AS sector_raw,
        COALESCE(u.value, 0)::numeric     AS value,
        COALESCE(u.marketcap, 0)::numeric AS marketcap,
        regexp_replace(
          replace(replace(replace(trim(lower(u.stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key
      FROM daily_union u
    ),

    daily_sector AS (
      SELECT
        d0.d,
        CASE
          -- هرجا sym_etf match شد، یعنی ETF است
          WHEN se.etf_bucket IS NOT NULL
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(se.etf_bucket, 'other')
          -- fallback: اگر اسم سکتور خودش صندوق بود
          WHEN d0.sector_raw ILIKE '%صندوق%' AND d0.sector_raw ILIKE '%قابل معامله%'
            THEN 'صندوق سرمایه گذاری قابل معامله | other'
          ELSE d0.sector_raw
        END AS sector,
        SUM(d0.value)::numeric     AS total_value,
        SUM(d0.marketcap)::numeric AS marketcap
      FROM daily0 d0
      LEFT JOIN sym_etf se
        ON se.ticker_key = d0.ticker_key
      GROUP BY 1,2
    ),

    /* ----------------------------
      2) haghighi با symbol (تفکیک ETF)
    -----------------------------*/
    haghighi0 AS (
      SELECT
        h.recdate::date AS d,
        COALESCE(NULLIF(trim(h.sector), ''), 'other') AS sector_raw,
        (COALESCE(h.buy_i_value, 0) - COALESCE(h.sell_i_value, 0))::numeric AS net_real_value,
        regexp_replace(
          replace(replace(replace(trim(lower(h.symbol)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key
      FROM haghighi h
    ),

    haghighi_sector AS (
      SELECT
        h0.d,
        CASE
          WHEN se.etf_bucket IS NOT NULL
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(se.etf_bucket, 'other')
          WHEN h0.sector_raw ILIKE '%صندوق%' AND h0.sector_raw ILIKE '%قابل معامله%'
            THEN 'صندوق سرمایه گذاری قابل معامله | other'
          ELSE h0.sector_raw
        END AS sector,
        SUM(h0.net_real_value)::numeric AS net_real_value
      FROM haghighi0 h0
      LEFT JOIN sym_etf se
        ON se.ticker_key = h0.ticker_key
      GROUP BY 1,2
    ),

    /* ----------------------------
      3) merge
    -----------------------------*/
    base AS (
      SELECT
        ds.d,
        ds.sector,
        ds.total_value,
        ds.marketcap,
        COALESCE(hs.net_real_value, 0)::numeric AS net_real_value
      FROM daily_sector ds
      LEFT JOIN haghighi_sector hs
        ON hs.d = ds.d AND hs.sector = ds.sector
    )

    SELECT
      sector,
      d AS date_miladi,

      total_value,
      marketcap,
      net_real_value,

      AVG(total_value)             OVER w5  AS avg_value_5d,
      AVG(total_value)             OVER w20 AS avg_value_20d,
      AVG(total_value)             OVER w60 AS avg_value_60d,
      STDDEV_SAMP(total_value)     OVER w20 AS std_value_20d,

      AVG(net_real_value)          OVER w5  AS avg_real_5d,
      AVG(net_real_value)          OVER w20 AS avg_real_20d,
      STDDEV_SAMP(net_real_value)  OVER w20 AS std_net_real_20d
    FROM base
    WINDOW
      w5  AS (PARTITION BY sector ORDER BY d ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
      w20 AS (PARTITION BY sector ORDER BY d ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
      w60 AS (PARTITION BY sector ORDER BY d ROWS BETWEEN 59 PRECEDING AND CURRENT ROW);
    """)

    op.execute(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{MV_NAME}_sector_date
    ON {MV_NAME} (sector, date_miladi);
    """)

    op.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_{MV_NAME}_date_desc
    ON {MV_NAME} (date_miladi DESC);
    """)

    op.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_{MV_NAME}_sector_date_desc
    ON {MV_NAME} (sector, date_miladi DESC);
    """)


def downgrade():
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV_NAME};")
