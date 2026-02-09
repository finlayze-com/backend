"""create mv_Relative Strength

Revision ID: 42a3db6c2fcf
Revises: f44b132d9d3e
Create Date: 2026-02-09 09:41:29.117895

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '42a3db6c2fcf'
down_revision: Union[str, Sequence[str], None] = 'f44b132d9d3e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sector_relative_strength AS
    WITH base AS (
      SELECT
        d.date_miladi::date AS date_miladi,
        COALESCE(NULLIF(trim(d.sector), ''), 'other') AS sector,
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
    market_daily AS (
      SELECT
        date_miladi,
        AVG(ret_1d) AS market_ret_1d
      FROM rets
      WHERE ret_1d IS NOT NULL
      GROUP BY 1
    ),
    joined AS (
      SELECT
        s.date_miladi,
        s.sector,
        s.sector_ret_1d,
        m.market_ret_1d
      FROM sector_daily s
      JOIN market_daily m USING (date_miladi)
    ),
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
