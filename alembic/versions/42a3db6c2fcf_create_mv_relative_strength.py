"""create mv_sector_relative_strength (with ETF buckets, market = stocks only)

Revision ID: 42a3db6c2fcf
Revises: f44b132d9d3e
Create Date: 2026-02-09 09:41:29.117895
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '42a3db6c2fcf'
down_revision: Union[str, Sequence[str], None] = 'f44b132d9d3e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_sector_relative_strength;
    """)

    op.execute(r"""
    CREATE MATERIALIZED VIEW mv_sector_relative_strength AS
    WITH
    /* ----------------------------
      0) ETF mapping از symboldetail (مثل mv_sector_daily_latest)
    -----------------------------*/
    sym_etf_raw AS (
      SELECT
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        NULLIF(trim(subsector), '') AS subsector_raw,
        NULLIF(trim(instrument_type), '') AS instrument_type
      FROM public.symboldetail
      WHERE sector = 'صندوق سرمايه گذاري قابل معامله'
        AND market <> 'بازار مشتقه'
    ),
    sym_etf_norm AS (
      SELECT
        ticker_key,
        instrument_type,
        regexp_replace(
          regexp_replace(
            replace(replace(replace(trim(lower(COALESCE(subsector_raw,''))), 'ي','ی'),'ك','ک'), chr(8204), ''),
            '\s*:\s*', ' : ', 'g'
          ),
          '\s+', ' ', 'g'
        ) AS subsector_clean
      FROM sym_etf_raw
    ),
    etf_bucket_map AS (
      SELECT
        ticker_key,
        CASE
          WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%'
            THEN 'املاک و مستغلات'

          -- سهامی شاخصی قبل از سهامی
          WHEN (subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامي%')
           AND (subsector_clean ILIKE '%شاخص%' OR subsector_clean ILIKE '%شاخصي%')
            THEN 'سهامی شاخصی'

          WHEN subsector_clean ILIKE '%اهرم%' THEN 'اهرمـی'
          WHEN subsector_clean ILIKE '%بخشی%' THEN 'بخشی'

          WHEN subsector_clean ILIKE '%درآمد ثابت%'
            OR subsector_clean ILIKE '%در امد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوارق بهادار با درآمد ثابت%'
            OR subsector_clean ILIKE '%در اوراق بهادار با%درآمد ثابت%'
            THEN 'درآمد ثابت'

          WHEN subsector_clean ILIKE '%مختلط%' THEN 'مختلط'
          WHEN subsector_clean ILIKE '%طلا%' OR subsector_clean ILIKE '%سکه%' THEN 'طلا'
          WHEN subsector_clean ILIKE '%کالا%' OR subsector_clean ILIKE '%commodity%' THEN 'کالایی'
          WHEN subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامی%' OR subsector_clean ILIKE '%سهامي%'
            THEN 'سهامی'
          ELSE NULL
        END AS bucket_from_subsector,
        CASE
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '') AND instrument_type = 'fund_gold'
            THEN 'طلا'
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '') AND instrument_type = 'fund_zafran'
            THEN 'زعفران'
          ELSE NULL
        END AS bucket_from_instrument
      FROM sym_etf_norm
      GROUP BY 1,2,3
    ),

    /* ----------------------------
      1) UNION داده‌ها: سهام + تمام صندوق‌ها (برای sector_ret)
      - src برای fallback طلا/زعفران وقتی subsector خالیه
    -----------------------------*/
    daily_union AS (
      SELECT
        'stock'::text AS src,
        d.date_miladi::date AS date_miladi,
        d.stock_ticker,
        COALESCE(NULLIF(trim(d.sector), ''), 'other') AS sector_raw,
        d.close::numeric AS close,
        COALESCE(d.is_temp,false) AS is_temp,
        regexp_replace(
          replace(replace(replace(trim(lower(d.stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key
      FROM daily_joined_data d
      WHERE COALESCE(d.is_temp,false) = false
        AND d.close IS NOT NULL

      UNION ALL
      SELECT 'fund_balanced', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_balanced
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_fixincome', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_fixincome
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_gold', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_gold
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_index_stock', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_index_stock
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_leverage', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_leverage
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_other', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_other
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_segment', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_segment
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_stock', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_stock
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL

      UNION ALL
      SELECT 'fund_zafran', date_miladi::date, stock_ticker, COALESCE(NULLIF(trim(sector),''),'other'), close::numeric, COALESCE(is_temp,false),
        regexp_replace(replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''), '\s+','', 'g')
      FROM daily_joined_fund_zafran
      WHERE COALESCE(is_temp,false)=false AND close IS NOT NULL
    ),

    /* ----------------------------
      2) sector_final برای ETF ها
    -----------------------------*/
    labeled AS (
      SELECT
        u.date_miladi,
        u.stock_ticker,
        u.close,
        u.src,
        CASE
          WHEN u.src <> 'stock' THEN
            'صندوق سرمایه گذاری قابل معامله | ' ||
            COALESCE(
              COALESCE(eb.bucket_from_subsector, eb.bucket_from_instrument),
              CASE
                WHEN u.src = 'fund_gold' THEN 'طلا'
                WHEN u.src = 'fund_zafran' THEN 'زعفران'
                ELSE NULL
              END,
              'other'
            )
          ELSE u.sector_raw
        END AS sector_final
      FROM daily_union u
      LEFT JOIN etf_bucket_map eb
        ON eb.ticker_key = u.ticker_key
    ),

    /* ----------------------------
      3) بازده 1 روزه نمادها + میانگین روزانه sector
      (برای همه: سهام + ETF)
    -----------------------------*/
    base AS (
      SELECT
        date_miladi,
        sector_final AS sector,
        stock_ticker,
        close,
        LAG(close) OVER (
          PARTITION BY stock_ticker
          ORDER BY date_miladi
        ) AS prev_close
      FROM labeled
    ),
    rets AS (
      SELECT
        date_miladi,
        sector,
        stock_ticker,
        CASE
          WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
          ELSE (close - prev_close) / prev_close
        END AS ret_1d
      FROM base
    ),
    sector_daily AS (
      SELECT
        date_miladi,
        sector,
        AVG(ret_1d) AS sector_ret_1d
      FROM rets
      WHERE ret_1d IS NOT NULL
      GROUP BY 1,2
    ),

    /* ----------------------------
      4) بازار فقط از سهام (بدون صندوق‌ها)
    -----------------------------*/
    market_only_stocks AS (
      SELECT
        d.date_miladi::date AS date_miladi,
        d.stock_ticker,
        d.close::numeric AS close,
        LAG(d.close::numeric) OVER (
          PARTITION BY d.stock_ticker
          ORDER BY d.date_miladi::date
        ) AS prev_close
      FROM daily_joined_data d
      WHERE COALESCE(d.is_temp,false) = false
        AND d.close IS NOT NULL
    ),
    market_rets AS (
      SELECT
        date_miladi,
        CASE
          WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
          ELSE (close - prev_close) / prev_close
        END AS ret_1d
      FROM market_only_stocks
    ),
    market_daily AS (
      SELECT
        date_miladi,
        AVG(ret_1d) AS market_ret_1d
      FROM market_rets
      WHERE ret_1d IS NOT NULL
      GROUP BY 1
    ),

    /* ----------------------------
      5) join sector_daily با market_daily
    -----------------------------*/
    joined AS (
      SELECT
        s.date_miladi,
        s.sector,
        s.sector_ret_1d,
        m.market_ret_1d
      FROM sector_daily s
      JOIN market_daily m USING (date_miladi)
    ),

    /* ----------------------------
      6) محاسبه cumulative return و RS
    -----------------------------*/
    roll AS (
      SELECT
        date_miladi,
        sector,
        sector_ret_1d,
        market_ret_1d,

        EXP(SUM(LN(1 + sector_ret_1d)) OVER w5)  - 1 AS sector_cumret_5d,
        EXP(SUM(LN(1 + market_ret_1d)) OVER w5)  - 1 AS market_cumret_5d,

        EXP(SUM(LN(1 + sector_ret_1d)) OVER w20) - 1 AS sector_cumret_20d,
        EXP(SUM(LN(1 + market_ret_1d)) OVER w20) - 1 AS market_cumret_20d,

        EXP(SUM(LN(1 + sector_ret_1d)) OVER w60) - 1 AS sector_cumret_60d,
        EXP(SUM(LN(1 + market_ret_1d)) OVER w60) - 1 AS market_cumret_60d

      FROM joined
      WINDOW
        w5  AS (PARTITION BY sector ORDER BY date_miladi ROWS BETWEEN 4  PRECEDING AND CURRENT ROW),
        w20 AS (PARTITION BY sector ORDER BY date_miladi ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
        w60 AS (PARTITION BY sector ORDER BY date_miladi ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
    )
    SELECT
      date_miladi,
      sector,

      sector_ret_1d,
      market_ret_1d,

      sector_cumret_5d,
      market_cumret_5d,
      ((1 + sector_cumret_5d)  / NULLIF(1 + market_cumret_5d,  0)) - 1 AS rs_5d,

      sector_cumret_20d,
      market_cumret_20d,
      ((1 + sector_cumret_20d) / NULLIF(1 + market_cumret_20d, 0)) - 1 AS rs_20d,

      sector_cumret_60d,
      market_cumret_60d,
      ((1 + sector_cumret_60d) / NULLIF(1 + market_cumret_60d, 0)) - 1 AS rs_60d
    FROM roll
    ORDER BY date_miladi DESC, rs_20d DESC;
    """)

    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_sector_relative_strength_date_sector
    ON mv_sector_relative_strength (date_miladi, sector);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_relative_strength_date
    ON mv_sector_relative_strength (date_miladi DESC);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_relative_strength_rs20
    ON mv_sector_relative_strength (rs_20d DESC);
    """)


def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_sector_relative_strength;")
