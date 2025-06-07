import requests
import pandas as pd
import json
import time
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# مسیرها
id_file_path = 'backend/Document/id.txt'
output_excel_path = 'backend/Document/finallist.xlsx'
failed_ids_path = 'backend/Document/failed_inscodes.txt'

# بارگذاری env
load_dotenv()
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST', 'localhost')
db_name = os.getenv('DB_NAME')

# اتصال به پایگاه‌داده
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')

# خواندن کدها
with open(id_file_path, 'r') as f:
    id_list = [line.strip() for line in f if line.strip()]

final_data = []
failed_ids = []

for inscode in id_list:
    try:
        url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{inscode}"
        res = requests.get(url)
        res.raise_for_status()
        info = res.json()

        row = {
            "insCode": info.get("insCode"),
            "name": info.get("lVal30"),
            "name_en": info.get("lVal18"),
            "sector": info.get("sector", {}).get("lSecVal"),
            "sector_code": info.get("sector", {}).get("cSecVal"),
            "subsector": info.get("faraDesc"),
            "market": info.get("flowTitle"),
            "panel": info.get("cgrValCotTitle"),
            "stock_ticker": info.get("lVal18AFC"),
            "share_number": info.get("zTitad"),
            "base_vol": info.get("baseVol"),
            "instrumentID": info.get("instrumentID"),
        }
        final_data.append(row)
        print(f"🔁 Processing inscode: {inscode}")
        time.sleep(0.4)

    except Exception as e:
        failed_ids.append(inscode)
        print(f"❌ Failed for {inscode}: {e}")

# ذخیره فایل و پایگاه‌داده
df = pd.DataFrame(final_data)
df.to_excel(output_excel_path, index=False)
df.to_sql('symboldetail', engine, if_exists='replace', index=False)

if failed_ids:
    with open(failed_ids_path, 'w') as f:
        f.write('\n'.join(failed_ids))

print(f"✅ Done: {len(df)} rows saved. Failed: {len(failed_ids)}")
