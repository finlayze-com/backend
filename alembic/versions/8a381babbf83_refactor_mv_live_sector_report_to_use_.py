"""refactor mv_live_sector_report to use mv_symbol_market_map

Revision ID: 8a381babbf83
Revises: 9f0d82773c5b
Create Date: 2026-02-23 10:57:07.785825

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8a381babbf83'
down_revision: Union[str, Sequence[str], None] = '9f0d82773c5b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""refactor mv_live_sector_report to use mv_symbol_market_map (single source of truth)

Revision ID: <PUT_NEW_REVISION_ID>
Revises: 4296e7cb75c4
Create Date: 2026-02-23
"""

def upgrade():
    op.execute(r"""
    DROP MATERIALIZED VIEW IF EXISTS mv_live_sector_report;
    CREATE MATERIALIZED VIEW mv_live_sector_report AS
    WITH
    /* ----------------------------
      0) latest live timestamp
    -----------------------------*/
    latest_live AS (
      SELECT max("Download") AS ts
      FROM live_market_data
    ),

    /* ----------------------------
      1) last_daily anchor (برای محدود کردن prev_close)
    -----------------------------*/
    last_daily AS (
      SELECT max(date_miladi)::date AS d
      FROM daily_joined_data
    ),

    /* ----------------------------
      2) close union (stocks + all funds) -> prev_close
    -----------------------------*/
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

    /* ----------------------------
      3) base live rows (latest ts)
    -----------------------------*/
    base AS (
      SELECT
        l."Download" AS ts,
        l."Ticker"   AS stock_ticker,
        COALESCE(NULLIF(trim(l."Sector"), ''), 'unknown') AS sector_live,

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

    /* ----------------------------
      4) enrich with prev_close + map(sector_key, etf_bucket)
         ✅ منبع یکتا: mv_symbol_market_map
    -----------------------------*/
    base2 AS (
      SELECT
        b.*,
        d.prev_close,

        /* sector_key نهایی برای grouping/report */
        COALESCE(
          NULLIF(trim(m.sector_key), ''),
          /* fallback: اگر map نداشتیم ولی live گفت ETF بود */
          CASE
            WHEN b.sector_live = 'صندوق سرمایه گذاری قابل معامله'
              THEN 'صندوق سرمایه گذاری قابل معامله | ' || COALESCE(NULLIF(trim(m.etf_bucket), ''), 'other')
            ELSE COALESCE(NULLIF(trim(b.sector_live), ''), 'other')
          END
        ) AS sector_key_final

      FROM base b
      LEFT JOIN daily_last_close d
        ON d.ticker_key = b.ticker_key
      LEFT JOIN mv_symbol_market_map m
        ON m.ticker_key = b.ticker_key
    ),

    /* ----------------------------
      5) sector rows (group by sector_key_final)
    -----------------------------*/
    sector_rows AS (
      SELECT
        ts,
        'sector'::text AS level,
        sector_key_final AS key,
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
      GROUP BY ts, sector_key_final
    ),

    /* ----------------------------
      6) market row (all)
    -----------------------------*/
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

    # Indexes (برای سرعت query و REFRESH CONCURRENTLY)
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
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_live_sector_report;")