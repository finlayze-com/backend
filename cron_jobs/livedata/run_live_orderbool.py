import asyncio
import aiohttp
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sys
import time

# تنظیمات اتصال به دیتابیس
DB_URL = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"
engine = create_engine(DB_URL)

# گرفتن لیست نمادها و اطلاعات آنها از جدول symboldetail
def get_inscodes():
    query = """
        SELECT
            sd."insCode",
            sd."stock_ticker",
            sd."sector"
        FROM "symboldetail" sd
        JOIN (
            SELECT DISTINCT "Ticker"
            FROM live_market_data
            WHERE "Download"::date = CURRENT_DATE 
        ) lm
        ON lm."Ticker" = sd."stock_ticker"
    """
    return pd.read_sql(query, engine)


# گرفتن داده اردربوک برای یک نماد
async def fetch_orderbook(session, inscode, symbol, sector):
    url = f"https://cdn.tsetmc.com/api/BestLimits/{inscode}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 200:
                data = await response.json()
                rows = data.get("bestLimits", [])
                record = {
                    "insCode": inscode,
                    "Symbol": symbol,
                    "Sector": sector,
                    "Timestamp": datetime.datetime.now()
                }
                for i in range(5):
                    row = rows[i] if i < len(rows) else {}
                    record[f"BuyPrice{i+1}"] = row.get("pMeDem")
                    record[f"BuyVolume{i+1}"] = row.get("qTitMeDem")
                    record[f"SellPrice{i+1}"] = row.get("pMeOf")
                    record[f"SellVolume{i+1}"] = row.get("qTitMeOf")
                return record
    except Exception as e:
        print(f"❌ خطا برای {symbol}: {e}")
    return None

# گرفتن اردربوک همه نمادها
async def get_all_orderbooks(inscode_df):
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_orderbook(session, row["insCode"], row["stock_ticker"], row["sector"])
            for _, row in inscode_df.iterrows()
        ]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

# ذخیره در دیتابیس
def save_to_db(df):
    df.to_sql("orderbook_snapshot", engine, if_exists="append", index=False)
    print(f"✅ {len(df)} ردیف ذخیره شد در orderbook_snapshot")

# اجرای کامل یک بار ذخیره
def run_once():
    inscode_df = get_inscodes()
    if sys.version_info >= (3, 11):
        records = asyncio.run(get_all_orderbooks(inscode_df))
    else:
        loop = asyncio.get_event_loop()
        records = loop.run_until_complete(get_all_orderbooks(inscode_df))
    if records:
        df = pd.DataFrame(records)
        save_to_db(df)
    else:
        print("⚠️ هیچ داده‌ای دریافت نشد.")

if __name__ == "__main__":
    run_once()
