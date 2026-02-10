# -*- coding: utf-8 -*-
"""
Refresh ONLY live materialized views (every 5 minutes during market hours):
  - mv_live_sector_report
  - mv_orderbook_report
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

logger = logging.getLogger("refresh_live_mvs")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(handler)


def get_sync_db_url() -> str:
    db_url = os.getenv("DB_URL_SYNC") or os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("DB_URL or DB_URL_SYNC not set in env/.env")

    # convert async URL to psycopg2 if needed
    db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    db_url = db_url.replace("postgresql+asyncpg:", "postgresql+psycopg2:")
    return db_url


def main():
    started = datetime.utcnow()
    engine = create_engine(get_sync_db_url(), pool_pre_ping=True)

    logger.info("▶️ refreshing live MVs: mv_live_sector_report, mv_orderbook_report")

    with engine.begin() as conn:
        # اگر قفل/کندی دیدی، بعداً می‌ریم سمت CONCURRENTLY (نیاز به unique index روی MV داره)
        conn.execute(text("REFRESH MATERIALIZED VIEW mv_live_sector_report;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW mv_orderbook_report;"))

    elapsed = (datetime.utcnow() - started).total_seconds()
    logger.info("✅ live MVs refreshed. elapsed=%.2fs", elapsed)


if __name__ == "__main__":
    main()
