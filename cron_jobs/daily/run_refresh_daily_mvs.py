# -*- coding: utf-8 -*-
"""
Refresh NON-LIVE materialized views (daily / after market close)
MVs:
  - mv_market_daily_latest
  - mv_sector_daily_latest
  - mv_sector_baseline
  - mv_sector_relative_strength
  - mv_symbol_market_map

Safe:
  - checks MV existence in pg_matviews
  - refresh in a sensible order
"""

import os
import sys
import logging
from datetime import datetime
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logger = logging.getLogger("refresh_daily_mvs")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)


def get_sync_db_url() -> str:
    db_url = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("DB_URL or DB_URL_SYNC not set in env/.env")

    db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    db_url = db_url.replace("postgresql+asyncpg:", "postgresql+psycopg2:")
    return db_url


def _mv_exists(conn, mv_name: str, schema: str = "public") -> bool:
    q = text("""
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = :schema AND matviewname = :name
        LIMIT 1
    """)
    return conn.execute(q, {"schema": schema, "name": mv_name}).scalar() is not None


def _refresh(conn, mv_name: str, schema: str = "public"):
    full = f'{schema}.{mv_name}'
    logger.info("🔄 REFRESH %s", full)
    conn.execute(text(f"REFRESH MATERIALIZED VIEW {full};"))


def main():
    schema = os.getenv("MV_SCHEMA", "public")  # اگر لازم شد از env تغییر می‌دی
    engine = create_engine(get_sync_db_url(), pool_pre_ping=True)

    started = datetime.utcnow()
    logger.info("▶️ refreshing DAILY MVs (schema=%s)", schema)

    # ترتیب پیشنهادی:
    # 1) map
    # 2) daily_latest ها
    # 3) baseline
    # 4) relative strength
    mv_list = [
        "mv_symbol_market_map",
        "mv_market_daily_latest",
        "mv_sector_daily_latest",
        "mv_sector_baseline",
        "mv_sector_relative_strength",
    ]

    refreshed = []
    skipped = []

    with engine.begin() as conn:
        for mv in mv_list:
            if _mv_exists(conn, mv, schema=schema):
                _refresh(conn, mv, schema=schema)
                refreshed.append(mv)
            else:
                logger.warning("⚠️ MV not found, skipped: %s.%s", schema, mv)
                skipped.append(mv)

    elapsed = (datetime.utcnow() - started).total_seconds()
    logger.info("✅ DAILY MVs refreshed=%s skipped=%s elapsed=%.2fs", refreshed, skipped, elapsed)


if __name__ == "__main__":
    main()
