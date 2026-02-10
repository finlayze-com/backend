# -*- coding: utf-8 -*-
"""
Save intraday snapshots every 5 minutes into:
  - market_intraday_snapshot
  - sector_intraday_snapshot

Source:
  - mv_live_sector_report
  - mv_orderbook_report  (optional for sector; required for __ALL__)

Key Fix:
  - DO NOT use m.ts from MV (it can be stale)
  - Use DB now() as snapshot ts so each run creates a new timestamp

Safe:
  - ON CONFLICT DO NOTHING
  - logs to stdout (captured by scheduler main)

Env:
  - DB_URL (preferred)  OR  DB_URL_SYNC
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

# Optional dotenv (same pattern as your project)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger("intraday_snapshots")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)


def _get_sync_db_url() -> str:
    """
    Priority:
      1) DB_URL_SYNC (psycopg2)
      2) DB_URL (convert asyncpg -> psycopg2)
    """
    db_url_sync = os.getenv("DB_URL_SYNC")
    if db_url_sync:
        return db_url_sync

    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("DB_URL or DB_URL_SYNC is not set in env/.env")

    # If async URL, convert to sync for psycopg2
    db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    db_url = db_url.replace("postgresql+asyncpg:", "postgresql+psycopg2:")
    return db_url


# IMPORTANT:
#  - Use now() AS ts (DB time) instead of m.ts from MV
#  - snapshot_day derived from now() as well
SQL_INSERT_MARKET = """
INSERT INTO market_intraday_snapshot (
  ts, snapshot_day,
  symbols_count, green_ratio, eqw_avg_ret_pct,
  total_value, total_volume, net_real_value, net_legal_value,
  imbalance5, imbalance_state,
  source
)
SELECT
  now() AS ts,
  (now() AT TIME ZONE 'Asia/Tehran')::date AS snapshot_day,
  m.symbols_count, m.green_ratio, m.eqw_avg_ret_pct,
  m.total_value, m.total_volume, m.net_real_value, m.net_legal_value,
  ob.imbalance5, ob.imbalance_state,
  'mv_pipeline'
FROM mv_live_sector_report m
LEFT JOIN mv_orderbook_report ob
  ON ob.sector = '__ALL__'
WHERE m.level = 'market' AND m.key = '__ALL__';
"""

SQL_INSERT_SECTOR = """
INSERT INTO sector_intraday_snapshot (
  ts, snapshot_day,
  sector_key, sector_name,
  symbols_count, green_ratio,
  total_value, total_volume, net_real_value, net_legal_value,
  imbalance5, imbalance_state
)
SELECT
  now() AS ts,
  (now() AT TIME ZONE 'Asia/Tehran')::date AS snapshot_day,
  m.key AS sector_key,
  m.key AS sector_name,
  m.symbols_count, m.green_ratio,
  m.total_value, m.total_volume, m.net_real_value, m.net_legal_value,
  ob.imbalance5, ob.imbalance_state
FROM mv_live_sector_report m
LEFT JOIN mv_orderbook_report ob
  ON ob.sector = m.key
WHERE m.level = 'sector'
ON CONFLICT (ts, sector_key) DO NOTHING;
"""


def main():
    db_url = _get_sync_db_url()
    engine = create_engine(db_url, pool_pre_ping=True)

    started = datetime.now()  # local wall clock for elapsed (ok)
    logger.info("▶️ intraday snapshot job started")

    with engine.begin() as conn:
        # market: with now() ts, conflict is extremely unlikely.
        # (we still don't add ON CONFLICT for ts because ts is unique by nature;
        #  if you want absolute safety, we can add ON CONFLICT(ts) DO NOTHING)
        r1 = conn.execute(text(SQL_INSERT_MARKET))

        # sector: uses PK(ts, sector_key) so conflict-safe
        r2 = conn.execute(text(SQL_INSERT_SECTOR))

        # Optional: report the "current" snapshot ts as seen by DB (for debug)
        snap_ts = conn.execute(text("SELECT now()")).scalar()

    elapsed = (datetime.now() - started).total_seconds()
    logger.info(
        "✅ intraday snapshot job done. market_inserted=%s sector_inserted=%s db_now=%s elapsed=%.2fs",
        getattr(r1, "rowcount", None),
        getattr(r2, "rowcount", None),
        snap_ts,
        elapsed,
    )


if __name__ == "__main__":
    main()
