"""create mv_sector_baseline

Revision ID: d3af3297ca2b
Revises: 2e0542b2db24
Create Date: 2026-02-08 15:51:56.610569

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3af3297ca2b'
down_revision: Union[str, Sequence[str], None] = '2e0542b2db24'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


MV_NAME = "mv_sector_baseline"


def upgrade():
    # 1) Create materialized view
    op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {MV_NAME};")

    op.execute(f"""
    CREATE MATERIALIZED VIEW {MV_NAME} AS
    WITH
    daily_sector AS (
      SELECT
        d.date_miladi::date AS d,
        COALESCE(NULLIF(trim(d.sector), ''), 'other') AS sector,
        SUM(COALESCE(d.value, 0))::numeric     AS total_value,
        SUM(COALESCE(d.marketcap, 0))::numeric AS marketcap
      FROM daily_joined_data d
      GROUP BY 1, 2
    ),
    haghighi_sector AS (
      SELECT
        h.recdate::date AS d,
        COALESCE(NULLIF(trim(h.sector), ''), 'other') AS sector,
        SUM(COALESCE(h.buy_i_value, 0) - COALESCE(h.sell_i_value, 0))::numeric AS net_real_value
      FROM haghighi h
      GROUP BY 1, 2
    ),
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

    # 2) Indexes on MV
    # برای REFRESH CONCURRENTLY حتماً UNIQUE لازم است
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
