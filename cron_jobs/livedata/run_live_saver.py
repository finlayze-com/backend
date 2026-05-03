import time
from datetime import datetime, time as dt_time
import pandas as pd
import finpy_tse as fps
from sqlalchemy import create_engine
import os
from custom_marketwatch import Get_MarketWatch

os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

# تنظیمات پایگاه داده
DB_URI = "postgresql://postgres:Afiroozi12@localhost:5432/postgres1"
engine = create_engine(DB_URI)

# مسیر فایل لاگ
LOG_FILE = "log_live_market.txt"

def log(msg):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{now}] {msg}"
    print(full_msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")

def is_market_open():
    now = datetime.now().time()
    return dt_time(9, 0) <= now <= dt_time(13, 30)

def save_live_market_data():
    try:
        #df, _ = fps.Get_MarketWatch()
        df, _ = Get_MarketWatch()
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.reset_index()
            df['updated_at'] = datetime.now()
            print(df.columns.tolist())
            df["Download"] = datetime.now()
            df.to_sql("live_market_data", engine, if_exists='append', index=False)
            log(f"✅ ذخیره {len(df)} ردیف در {datetime.now().strftime('%H:%M:%S')}")
        else:
            log("⚠️ داده‌ای دریافت نشد یا ساختار نامعتبر بود.")
    except Exception as e:
        log(f"❌ خطا در ذخیره داده: {e}")

def main():
    # اجرای یک‌باره: اگر بازار باز بود ذخیره کن، وگرنه سریع خارج شو
    if is_market_open():
        log("🚀 اجرای یک‌باره: بازار باز است، ذخیره می‌کنم…")
        save_live_market_data()
    else:
        log("⏸️ اجرای یک‌باره: بازار بسته است، کاری انجام نشد.")

if __name__ == "__main__":
    main()

