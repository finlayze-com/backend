"""create mv_sector_daily_latest

Revision ID: f44b132d9d3e
Revises: d3af3297ca2b
Create Date: 2026-02-09 09:02:11.403940

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f44b132d9d3e'
down_revision: Union[str, Sequence[str], None] = 'd3af3297ca2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# -*- coding: utf-8 -*-
"""create mv_sector_daily_latest

Revision ID: <PUT_YOUR_REVISION_ID>
Revises: <PUT_DOWN_REVISION_ID>
Create Date: 2026-02-09
"""
def upgrade():
    # 1) اگر قبلاً وجود داشته، حذفش کن (برای idempotent بودن migration)
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_sector_daily_latest;
    """)

    # 2) ساخت MV
    op.execute("""
    CREATE MATERIALIZED VIEW mv_sector_daily_latest AS
    WITH last_day AS (
      SELECT MAX(date_miladi::date) AS d
      FROM daily_joined_data
      WHERE COALESCE(is_temp,false) = false
    ),
    daily_sector AS (
      SELECT
        d.date_miladi::date AS date_miladi,
        COALESCE(NULLIF(trim(d.sector), ''), 'other') AS sector,
        COUNT(DISTINCT d.stock_ticker) AS symbols_count,
        SUM(COALESCE(d.value, 0))::numeric     AS total_value,
        SUM(COALESCE(d.volume, 0))::numeric    AS total_volume,
        SUM(COALESCE(d.marketcap, 0))::numeric AS marketcap
      FROM daily_joined_data d
      JOIN last_day ld ON d.date_miladi::date = ld.d
      WHERE COALESCE(d.is_temp,false) = false
      GROUP BY 1,2
    ),
    haghighi_sector AS (
      SELECT
        h.recdate::date AS date_miladi,
        COALESCE(NULLIF(trim(h.sector), ''), 'other') AS sector,
        SUM(COALESCE(h.buy_i_value,0) - COALESCE(h.sell_i_value,0))::numeric AS net_real_value
      FROM haghighi h
      JOIN last_day ld ON h.recdate::date = ld.d
      WHERE COALESCE(h.is_temp,false) = false
      GROUP BY 1,2
    )
    SELECT
      ds.date_miladi,
      ds.sector,
      ds.symbols_count,
      ds.total_value,
      ds.total_volume,
      ds.marketcap,
      COALESCE(hs.net_real_value,0)::numeric AS net_real_value
    FROM daily_sector ds
    LEFT JOIN haghighi_sector hs
      ON hs.date_miladi = ds.date_miladi
     AND hs.sector = ds.sector;
    """)

    # 3) ایندکس‌ها روی MV (برای سرعت SELECT ها)
    # - Unique index برای REFRESH CONCURRENTLY در آینده
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_sector_daily_latest_date_sector
    ON mv_sector_daily_latest (date_miladi, sector);
    """)

    # - index برای sort/filter رایج
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_daily_latest_total_value
    ON mv_sector_daily_latest (total_value DESC);
    """)

    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_sector_daily_latest_net_real_value
    ON mv_sector_daily_latest (net_real_value DESC);
    """)

    # 4) اختیاری ولی مفید: آمار برای پلن بهتر
    op.execute("ANALYZE mv_sector_daily_latest;")


def downgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_sector_daily_latest;
    """)
