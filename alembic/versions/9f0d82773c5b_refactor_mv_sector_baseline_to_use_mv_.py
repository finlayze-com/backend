"""refactor mv_sector_baseline to use mv_symbol_market_map

Revision ID: 9f0d82773c5b
Revises: 56551dc91b1b
Create Date: 2026-02-23 09:15:52.442222

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9f0d82773c5b'
down_revision: Union[str, Sequence[str], None] = '56551dc91b1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


"""refactor mv_sector_baseline to use mv_symbol_market_map for sector/etf separation (no column changes)

Revision ID: d3af3297ca2b
Revises: 2e0542b2db24
Create Date: 2026-02-08 15:51:56.610569
"""
MV_NAME = "mv_sector_baseline"


def upgrade():
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV_NAME};")

    op.execute(rf"""
    CREATE MATERIALIZED VIEW {MV_NAME} AS
    WITH

    /* ==============================
       0) UNION: Stocks + ALL ETF tables
    ===============================*/
    daily_union AS (
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_data

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_balanced

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_fixincome

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_gold

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_index_stock

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_leverage

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_other

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_segment

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_stock

      UNION ALL
      SELECT date_miladi, stock_ticker, value, marketcap, is_temp
      FROM daily_joined_fund_zafran
    ),

    /* ==============================
       1) Normalize ticker_key
    ===============================*/
    daily0 AS (
      SELECT
        date_miladi::date AS d,
        regexp_replace(
          replace(replace(replace(trim(lower(stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        COALESCE(value, 0)::numeric     AS value,
        COALESCE(marketcap, 0)::numeric AS marketcap
      FROM daily_union
      WHERE COALESCE(is_temp,false) = false
    ),

    /* ==============================
       2) Join with mv_symbol_market_map (single source of truth)
    ===============================*/
    daily1 AS (
      SELECT
        d0.d,
        COALESCE(NULLIF(trim(m.sector_key), ''), NULLIF(trim(m.sector), ''), 'other') AS sector,
        d0.value,
        d0.marketcap
      FROM daily0 d0
      LEFT JOIN mv_symbol_market_map m
        ON m.ticker_key = d0.ticker_key
    ),

    daily_sector AS (
      SELECT
        d,
        sector,
        SUM(value)::numeric     AS total_value,
        SUM(marketcap)::numeric AS marketcap
      FROM daily1
      GROUP BY 1,2
    ),

    /* ==============================
       3) Haghighi (Real money)
    ===============================*/
    haghighi0 AS (
      SELECT
        h.recdate::date AS d,
        regexp_replace(
          replace(replace(replace(trim(lower(h.symbol)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        (COALESCE(h.buy_i_value, 0) - COALESCE(h.sell_i_value, 0))::numeric AS net_real_value
      FROM haghighi h
      WHERE COALESCE(h.is_temp,false) = false
    ),

    haghighi1 AS (
      SELECT
        h0.d,
        COALESCE(NULLIF(trim(m.sector_key), ''), NULLIF(trim(m.sector), ''), 'other') AS sector,
        h0.net_real_value
      FROM haghighi0 h0
      LEFT JOIN mv_symbol_market_map m
        ON m.ticker_key = h0.ticker_key
    ),

    haghighi_sector AS (
      SELECT
        d,
        sector,
        SUM(net_real_value)::numeric AS net_real_value
      FROM haghighi1
      GROUP BY 1,2
    ),

    /* ==============================
       4) Merge Daily + Real Money
    ===============================*/
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

    /* ==============================
       5) Baseline Metrics (unchanged)
    ===============================*/
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

    # Indexes
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