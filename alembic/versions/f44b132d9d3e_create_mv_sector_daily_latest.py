"""create mv_sector_daily_latest (with ETF buckets)

Revision ID: f44b132d9d3e
Revises: d3af3297ca2b
Create Date: 2026-02-09 09:02:11.403940
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'f44b132d9d3e'
down_revision: Union[str, Sequence[str], None] = 'd3af3297ca2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_sector_daily_latest;
    """)

    op.execute(r"""
    CREATE MATERIALIZED VIEW mv_sector_daily_latest AS
    WITH
    /* ----------------------------
      1) آخرین روز "واقعی" بازار (همان منطق قبلی)
    -----------------------------*/
    last_day AS (
      SELECT MAX(date_miladi::date) AS d
      FROM daily_joined_data
      WHERE COALESCE(is_temp,false) = false
    ),

    /* ----------------------------
      2) UNION سهام + همه صندوق‌ها (طبق اسکرین‌شات)
      - src کمک می‌کند بفهمیم رکورد از کدام نوع صندوق آمده
    -----------------------------*/
    daily_union AS (
      -- stock (سهام)
      SELECT
        'stock'::text AS src,
        d.date_miladi::date AS date_miladi,
        d.stock_ticker,
        d.sector,
        d.value, d.volume, d.marketcap,
        d.is_temp
      FROM daily_joined_data d

      UNION ALL
      SELECT 'fund_balanced', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_balanced

      UNION ALL
      SELECT 'fund_fixincome', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_fixincome

      UNION ALL
      SELECT 'fund_gold', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_gold

      UNION ALL
      SELECT 'fund_index_stock', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_index_stock

      UNION ALL
      SELECT 'fund_leverage', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_leverage

      UNION ALL
      SELECT 'fund_other', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_other

      UNION ALL
      SELECT 'fund_segment', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_segment

      UNION ALL
      SELECT 'fund_stock', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_stock

      UNION ALL
      SELECT 'fund_zafran', date_miladi::date, stock_ticker, sector, value, volume, marketcap, is_temp
      FROM daily_joined_fund_zafran
    ),

    /* ----------------------------
      3) نرمال‌سازی ticker_key برای join (مثل کدهای قبلی)
    -----------------------------*/
    daily0 AS (
      SELECT
        u.src,
        u.date_miladi,
        u.stock_ticker,
        COALESCE(NULLIF(trim(u.sector), ''), 'other') AS sector_raw,
        COALESCE(u.value,0)::numeric     AS value,
        COALESCE(u.volume,0)::numeric    AS volume,
        COALESCE(u.marketcap,0)::numeric AS marketcap,
        regexp_replace(
          replace(replace(replace(trim(lower(u.stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key
      FROM daily_union u
      JOIN last_day ld ON u.date_miladi = ld.d
      WHERE COALESCE(u.is_temp,false) = false
    ),

    /* ----------------------------
      4) mapping ETF از symboldetail (subsector + instrument_type)
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

    /* ----------------------------
      5) bucket کردن ETF ها (با fallback به src برای gold/zafran)
    -----------------------------*/
    etf_bucket_map AS (
      SELECT
        ticker_key,
        CASE
          -- اولویت‌های دقیق‌تر
          WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%' THEN 'املاک و مستغلات'

          -- سهامی شاخصی قبل از سهامی (تا داخل سهامی نیاد)
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

          WHEN subsector_clean ILIKE '%طلا%' OR subsector_clean ILIKE '%سکه%'
            THEN 'طلا'

          WHEN subsector_clean ILIKE '%کالا%' OR subsector_clean ILIKE '%commodity%'
            THEN 'کالایی'

          WHEN subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامی%' OR subsector_clean ILIKE '%سهامي%'
            THEN 'سهامی'

          ELSE NULL
        END AS bucket_from_subsector,

        -- برای وقتی subsector خالیه، instrument_type کمک می‌کنه
        CASE
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '') AND instrument_type = 'fund_gold' THEN 'طلا'
          WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '') AND instrument_type = 'fund_zafran' THEN 'زعفران'
          ELSE NULL
        END AS bucket_from_instrument
      FROM sym_etf_norm
      GROUP BY 1,2,3
    ),

    /* ----------------------------
      6) ساخت sector نهایی:
         - اگر رکورد از fund_* آمده => ETF است و باید bucket شود
         - bucket اولویت: subsector > instrument_type > src(fund_gold/zafran) > other
    -----------------------------*/
    daily_labeled AS (
      SELECT
        d0.date_miladi,
        d0.stock_ticker,
        d0.value, d0.volume, d0.marketcap,
        d0.src,
        d0.sector_raw,
        COALESCE(
          eb.bucket_from_subsector,
          eb.bucket_from_instrument,
          CASE
            WHEN d0.src = 'fund_gold'   THEN 'طلا'
            WHEN d0.src = 'fund_zafran' THEN 'زعفران'
            ELSE NULL
          END
        ) AS etf_bucket,

        CASE
          WHEN d0.src <> 'stock'
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(
                  COALESCE(
                    eb.bucket_from_subsector,
                    eb.bucket_from_instrument,
                    CASE
                      WHEN d0.src = 'fund_gold'   THEN 'طلا'
                      WHEN d0.src = 'fund_zafran' THEN 'زعفران'
                      ELSE NULL
                    END
                  ),
                  'other'
                )
          ELSE d0.sector_raw
        END AS sector_final
      FROM daily0 d0
      LEFT JOIN etf_bucket_map eb
        ON eb.ticker_key = d0.ticker_key
    ),

    /* ----------------------------
      7) تجمیع روز آخر بر اساس sector_final
    -----------------------------*/
    daily_sector AS (
      SELECT
        date_miladi,
        sector_final AS sector,
        COUNT(DISTINCT stock_ticker) AS symbols_count,
        SUM(value)::numeric     AS total_value,
        SUM(volume)::numeric    AS total_volume,
        SUM(marketcap)::numeric AS marketcap
      FROM daily_labeled
      GROUP BY 1,2
    ),

    /* ----------------------------
      8) haghighi هم باید به همان sector_final برسد (برای ETFها)
          - haghighi symbol -> ticker_key
          - bucket از symboldetail (اگر نبود => other)
    -----------------------------*/
    hag0 AS (
      SELECT
        h.recdate::date AS date_miladi,
        regexp_replace(
          replace(replace(replace(trim(lower(h.symbol)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        COALESCE(NULLIF(trim(h.sector), ''), 'other') AS sector_raw,
        (COALESCE(h.buy_i_value,0) - COALESCE(h.sell_i_value,0))::numeric AS net_real_value
      FROM haghighi h
      JOIN last_day ld ON h.recdate::date = ld.d
      WHERE COALESCE(h.is_temp,false) = false
    ),
    hag_labeled AS (
      SELECT
        h0.date_miladi,
        CASE
          WHEN eb.ticker_key IS NOT NULL
            THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(
                  COALESCE(eb.bucket_from_subsector, eb.bucket_from_instrument),
                  'other'
                )
          ELSE h0.sector_raw
        END AS sector_final,
        h0.net_real_value
      FROM hag0 h0
      LEFT JOIN etf_bucket_map eb
        ON eb.ticker_key = h0.ticker_key
    ),
    hag_sector AS (
      SELECT
        date_miladi,
        sector_final AS sector,
        SUM(net_real_value)::numeric AS net_real_value
      FROM hag_labeled
      GROUP BY 1,2
    )

    /* ----------------------------
      9) خروجی نهایی MV
    -----------------------------*/
    SELECT
      ds.date_miladi,
      ds.sector,
      ds.symbols_count,
      ds.total_value,
      ds.total_volume,
      ds.marketcap,
      COALESCE(hs.net_real_value,0)::numeric AS net_real_value
    FROM daily_sector ds
    LEFT JOIN hag_sector hs
      ON hs.date_miladi = ds.date_miladi
     AND hs.sector = ds.sector;
    """)

    # Indexes
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_sector_daily_latest_date_sector
    ON mv_sector_daily_latest (date_miladi, sector);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_daily_latest_total_value
    ON mv_sector_daily_latest (total_value DESC);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_daily_latest_net_real_value
    ON mv_sector_daily_latest (net_real_value DESC);
    """)

    op.execute("ANALYZE mv_sector_daily_latest;")


def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_sector_daily_latest;")
