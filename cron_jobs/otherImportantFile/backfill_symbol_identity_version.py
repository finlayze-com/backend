# scripts/backfill_symbol_identity_version.py
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import psycopg2
from psycopg2.extras import execute_batch

# پیدا کردن .env
env_path = find_dotenv(filename=".env", usecwd=True) or str(Path(__file__).resolve().parents[2] / ".env")
load_dotenv(env_path)

def main():
    db_url = os.getenv("DB_URL_SYNC")
    if not db_url:
        raise RuntimeError("DB_URL_SYNC not set in .env")

    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        # 1) پر کردن symbol_identity بر اساس stock_ticker
        print("➡️ inserting into symbol_identity ...")
        sql_identity = """
            INSERT INTO symbol_identity (stock_ticker, name)
            SELECT DISTINCT stock_ticker, name
            FROM symboldetail
            WHERE stock_ticker IS NOT NULL
            ON CONFLICT (stock_ticker) DO NOTHING;
        """
        cur.execute(sql_identity)
        print("✅ symbol_identity upsert done")

        # 2) مپ stock_ticker -> symbol_id
        cur.execute("SELECT symbol_id, stock_ticker FROM symbol_identity;")
        sid_by_ticker = {row[1]: row[0] for row in cur.fetchall()}

        # 3) خواندن ردیف‌ها از symboldetail
        cur.execute("""
            SELECT "insCode", stock_ticker, name, name_en, sector, sector_code,
                   subsector, market, panel, "instrumentID", share_number, base_vol
            FROM symboldetail
            WHERE stock_ticker IS NOT NULL AND "insCode" IS NOT NULL
        """)
        rows = cur.fetchall()

        # 4) ساخت batch برای symbol_version
        batch = []
        for (inscode, stock_ticker, name, name_en, sector, sector_code,
             subsector, market, panel, instrumentID, share_number, base_vol) in rows:
            sid = sid_by_ticker.get(stock_ticker)
            if not sid:
                continue
            batch.append((
                sid,
                int(inscode),
                stock_ticker,
                name, name_en, sector, sector_code, subsector, market, panel,
                instrumentID, share_number, base_vol
            ))

        print(f"➡️ inserting/updating {len(batch)} rows into symbol_version ...")
        sql_version = """
            INSERT INTO symbol_version (
                symbol_id, inscode, stock_ticker, start_date, end_date, is_active,
                name, name_en, sector, sector_code, subsector, market, panel,
                instrumentid, share_number, base_vol
            )
            VALUES (
                %s, %s, %s, CURRENT_DATE, NULL, TRUE,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (inscode) DO UPDATE SET
                symbol_id    = EXCLUDED.symbol_id,
                stock_ticker = EXCLUDED.stock_ticker,
                name         = EXCLUDED.name,
                name_en      = EXCLUDED.name_en,
                sector       = EXCLUDED.sector,
                sector_code  = EXCLUDED.sector_code,
                subsector    = EXCLUDED.subsector,
                market       = EXCLUDED.market,
                panel        = EXCLUDED.panel,
                instrumentid = EXCLUDED.instrumentid,
                share_number = EXCLUDED.share_number,
                base_vol     = EXCLUDED.base_vol;
        """
        if batch:
            execute_batch(cur, sql_version, batch, page_size=500)
        print("✅ symbol_version upsert done")
        conn.commit()

if __name__ == "__main__":
    main()
