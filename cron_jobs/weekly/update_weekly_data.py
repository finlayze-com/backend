# -*- coding: utf-8 -*-
"""
رانر اصلی ساخت همهٔ جداول هفتگی از روی جداول روزانه.
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from weekly.common.config import WEEKLY_SOURCES, WEEK_FREQ, DEFAULT_CONFLICT_KEYS
from weekly.common.base_weekly_updater import build_one_weekly_table

def main():
    load_dotenv()
    db_url = os.getenv("DB_URL") or os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL not set in .env")

    engine = create_engine(db_url)

    print("🚀 Weekly aggregation started")
    for weekly_table, daily_src in WEEKLY_SOURCES.items():
        try:
            build_one_weekly_table(
                weekly_table,
                source_table=daily_src,
                engine=engine,
                week_freq=WEEK_FREQ,
                conflict_keys=DEFAULT_CONFLICT_KEYS,
            )
        except Exception as e:
            print(f"❌ Failed: {weekly_table} ← {daily_src} | {e}")
    print("✅ Weekly aggregation finished")

if __name__ == "__main__":
    main()
