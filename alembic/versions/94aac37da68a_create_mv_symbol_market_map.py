"""create_mv_symbol_market_map (with ETF bucket)

Revision ID: 94aac37da68a
Revises: 42a3db6c2fcf
Create Date: 2026-02-09 10:50:26.784815
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = '94aac37da68a'
down_revision: Union[str, Sequence[str], None] = '42a3db6c2fcf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_symbol_market_map CASCADE;
    """)

    op.execute(r"""
    CREATE MATERIALIZED VIEW mv_symbol_market_map AS
    WITH sd0 AS (
      SELECT
        sd."insCode"::bigint AS "insCode",
        sd."stock_ticker" AS stock_ticker,

        regexp_replace(
          replace(replace(replace(trim(lower(sd."stock_ticker")), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,

        COALESCE(NULLIF(trim(sd."sector"), ''), 'other') AS sector,
        COALESCE(NULLIF(trim(sd."market"), ''), 'other') AS market,

        NULLIF(trim(sd."subsector"), '') AS subsector_raw,
        NULLIF(trim(sd."instrument_type"), '') AS instrument_type
      FROM public.symboldetail sd
      WHERE sd."insCode" IS NOT NULL
    ),

    sd1 AS (
      SELECT
        *,
        -- تمیزکاری subsector برای سرچ بهتر
        regexp_replace(
          regexp_replace(
            replace(replace(replace(trim(lower(COALESCE(subsector_raw,''))), 'ي','ی'),'ك','ک'), chr(8204), ''),
            '\s*:\s*', ' : ', 'g'
          ),
          '\s+', ' ', 'g'
        ) AS subsector_clean
      FROM sd0
    ),

    classified AS (
      SELECT
        *,
        CASE
          WHEN sector = 'صندوق سرمايه گذاري قابل معامله'
               AND market <> 'بازار مشتقه'
          THEN
            CASE
              -- ✅ اگر subsector خالی بود، از instrument_type نتیجه بده
              WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
                   AND instrument_type = 'fund_gold'
                THEN 'طلا'
              WHEN (subsector_clean IS NULL OR trim(subsector_clean) = '')
                   AND instrument_type = 'fund_zafran'
                THEN 'زعفران'

              -- ✅ املاک و مستغلات (دقیق)
              WHEN subsector_clean ILIKE '%املاک%' AND subsector_clean ILIKE '%مستغلات%'
                THEN 'املاک و مستغلات'

              -- ✅ سهامی شاخصی (قبل از سهامی)
              WHEN (subsector_clean ILIKE '%سهام%' OR subsector_clean ILIKE '%سهامي%')
               AND (subsector_clean ILIKE '%شاخص%' OR subsector_clean ILIKE '%شاخصي%')
                THEN 'سهامی شاخصی'

              -- دسته‌بندی‌های عمومی
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

              ELSE 'other'
            END
          ELSE NULL
        END AS etf_bucket
      FROM sd1
    )

    SELECT
      "insCode",
      stock_ticker,
      ticker_key,
      sector,
      market,

      -- ✅ خروجی bucket
      etf_bucket,

      -- ✅ کلید آماده برای گزارش‌ها (همون شکل MV های قبلی)
      CASE
        WHEN etf_bucket IS NOT NULL
          THEN 'صندوق سرمایه گذاری قابل معامله | ' || etf_bucket
        WHEN sector ILIKE '%صندوق%' AND sector ILIKE '%قابل معامله%'
          THEN 'صندوق سرمایه گذاری قابل معامله | other'
        ELSE sector
      END AS sector_key

    FROM classified;
    """)

    # indexes
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_symbol_market_map_inscode
    ON mv_symbol_market_map ("insCode");
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_ticker_key
    ON mv_symbol_market_map (ticker_key);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_market
    ON mv_symbol_market_map (market);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_sector
    ON mv_symbol_market_map (sector);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_sector_key
    ON mv_symbol_market_map (sector_key);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_symbol_market_map_etf_bucket
    ON mv_symbol_market_map (etf_bucket);
    """)


def downgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_symbol_market_map CASCADE;
    """)
