import requests
import pandas as pd
import json
import time
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ⬇️ مسیر فایل‌ها روی سرور
id_file_path = '/root/backend/Document/id.txt'
output_excel_path = '/root/backend/Document/finallist.xlsx'
failed_ids_path = '/root/backend/Document/failed_inscodes.txt'

# ⬇️ بارگذاری اطلاعات اتصال دیتابیس از .env
load_dotenv()
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST', 'localhost')
db_name = os.getenv('DB_NAME')

# ⬇️ ساخت اتصال SQLAlchemy
engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}')

# ⬇️ خواندن inscodeها از فایل متنی
with open(id_file_path, 'r') as f:
    id_list = [line.strip() for line in f.readlines() if line.strip()]

final_data = []
failed_ids = []

# ⬇️ دریافت اطلاعات از API و ساخت دیتافریم
for inscode in id_list:
    try:
        url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{inscode}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("instrument", {})

        row = {
            "inscode": inscode,
            "symbol": data.get("lVal18AFC", ""),
            "name": data.get("lVal30", ""),
            "sector": data.get("cSecVal", ""),
            "sub_sector": data.get("cSoSecVal", ""),
            "market": data.get("cSoSecValTitle", ""),
            "share_count": data.get("zTitad", ""),
            "min_volume": data.get("qTitSakht", ""),
            "base_vol": data.get("baseVol", ""),
            "group_pe": data.get("groupPe", ""),
            "estimated_eps": data.get("estimatedEPS", ""),
        }
        final_data.append(row)
        time.sleep(0.4)  # جلوگیری از بلاک شدن توسط API

    except Exception as e:
        failed_ids.append(inscode)
        print(f"❌ Failed for {inscode}: {e}")

# ⬇️ تبدیل به دیتافریم
df = pd.DataFrame(final_data)

# ⬇️ ذخیره اکسل
df.to_excel(output_excel_path, index=False)
print(f"✅ فایل {output_excel_path} با {len(df)} ردیف ذخیره شد.")

# ⬇️ ذخیره در دیتابیس PostgreSQL
df.to_sql('symboldetail', con=engine, if_exists='replace', index=False)
print("✅ جدول symboldetail در پایگاه‌داده PostgreSQL پر شد.")

# ⬇️ ذخیره خطاها (در صورت وجود)
if failed_ids:
    with open(failed_ids_path, 'w') as f:
        f.write('\n'.join(failed_ids))
    print(f"⚠️ {len(failed_ids)} inscode با خطا مواجه شد و در failed_inscodes.txt ذخیره شد.")
