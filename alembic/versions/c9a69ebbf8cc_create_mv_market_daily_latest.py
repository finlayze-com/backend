"""create_mv_market_daily_latest

Revision ID: c9a69ebbf8cc
Revises: b5a09b4ecd0a
Create Date: 2026-02-09 11:09:39.814428

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c9a69ebbf8cc'
down_revision: Union[str, Sequence[str], None] = 'b5a09b4ecd0a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    DROP MATERIALIZED VIEW IF EXISTS mv_market_daily_latest;
    """)

    op.execute(r"""
    CREATE MATERIALIZED VIEW mv_market_daily_latest AS
    WITH last_day AS (
      SELECT MAX(date_miladi::date) AS d
      FROM daily_joined_data
      WHERE COALESCE(is_temp,false) = false
    ),
    base AS (
      SELECT
        d.date_miladi::date AS date_miladi,
        regexp_replace(
          replace(replace(replace(trim(lower(d.stock_ticker)), 'ي','ی'),'ك','ک'), chr(8204), ''),
          '\s+','', 'g'
        ) AS ticker_key,
        COALESCE(d.value,0)::numeric     AS value,
        COALESCE(d.volume,0)::numeric    AS volume,
        COALESCE(d.marketcap,0)::numeric AS marketcap
      FROM daily_joined_data d
      JOIN last_day ld ON d.date_miladi::date = ld.d
      WHERE COALESCE(d.is_temp,false) = false
    ),
    joined AS (
      SELECT
        b.date_miladi,
        COALESCE(m.market,'other') AS market,
        b.ticker_key,
        b.value, b.volume, b.marketcap
      FROM base b
      LEFT JOIN mv_symbol_market_map m
        ON m.ticker_key = b.ticker_key
    )
    SELECT
      date_miladi,
      market,
      COUNT(DISTINCT ticker_key) AS symbols_count,
      SUM(value)     AS total_value,
      SUM(volume)    AS total_volume,
      SUM(marketcap) AS marketcap
    FROM joined
    GROUP BY 1,2;
    """)

    # unique index برای امکان REFRESH ... CONCURRENTLY
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_market_daily_latest_date_market
    ON mv_market_daily_latest (date_miladi, market);
    """)

    # optional: کمک به sort/filter
    op.execute("""
    CREATE INDEX IF NOT EXISTS ix_mv_market_daily_latest_market
    ON mv_market_daily_latest (market);
    """)

    # refresh اولیه لازم نیست چون همین الان ساخته میشه و پره.
    # ولی اگر بعداً خواستی از CONCURRENTLY استفاده کنی، همین unique index کافیه.


def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_market_daily_latest;")
