import os, time, requests, csv, tempfile
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
import psycopg2

# .env
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)
DB_URL = os.getenv("DB_URL_SYNC")

# مسیرها
BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DOCUMENT_DIR = os.path.join(BASE_DIR, 'backend', 'Document')

# فقط نام فایل‌ها؛ مسیر کامل را بعداً با DOCUMENT_DIR می‌سازیم
FILES = {
    'saham.txt':   'saham',      # سهام
    'saham -R.txt': 'rights_issue',     #حق تقدم
    'saham -2.txt': 'retail',           #خرده فروشی
    'saham -4.txt': 'Block',            #بلوکی
    'fund_stock.txt': 'fund_stock',              # صندوق سهامی
    'fund_segment.txt': 'fund_segment',  # صندوق بخشی
    'fund_balanced.txt': 'fund_balanced',  # صندوق مختلط
    'fund_fixincome.txt': 'fund_fixincome',  # صندوق درامد ثابت
    'fund_gold.txt': 'fund_gold',  # صندوق طلا
    'fund_index_stock.txt': 'fund_index_stock',  # صندوق شاخصی
    'fund_leverage.txt': 'fund_leverage',  # صندوق شاخصی
    'fund_zafran.txt': 'fund_zafran',  # صندوق شاخصی
    'fund_other.txt': 'fund_other',  # صندوق شاخصی
    'option.txt':  'option',            # اختیار
    'kala.txt':    'commodity',         # کالایی
    'tamin.txt':   'bond',              # تامین مالی
}

def read_ids_with_type():
    rows = []
    for filename, inst_type in FILES.items():
        path = os.path.join(DOCUMENT_DIR, filename)
        if not os.path.exists(path):
            print(f"⚠️ فایل یافت نشد: {path}")
            continue
        with open(path, 'r', encoding='utf-8') as f:
            ids = [line.strip() for line in f if line.strip()]
        # هر id را همراه نوع ابزار و نام فایل منبع نگه داریم
        rows.extend([(ins, inst_type, filename) for ins in ids])
    return rows

def fetch_info(inscode):
    url = f"https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{inscode}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    info = (r.json() or {}).get("instrumentInfo", {}) or {}
    return {
        "insCode": info.get("insCode"),
        "name": info.get("lVal30"),
        "name_en": info.get("lVal18"),
        "sector": (info.get("sector") or {}).get("lSecVal"),
        "sector_code": (info.get("sector") or {}).get("cSecVal"),
        "subsector": info.get("faraDesc"),
        "market": info.get("flowTitle"),
        "panel": info.get("cgrValCotTitle"),
        "stock_ticker": info.get("lVal18AFC"),
        "share_number": info.get("zTitad"),
        "base_vol": info.get("baseVol"),
        "instrumentID": info.get("instrumentID"),
    }

def upsert_symboldetail(rows):
    # پوشش کامل ستون‌ها + source_file
    cols = [
        "insCode","name","name_en","sector","sector_code","subsector","market","panel",
        "stock_ticker","share_number","base_vol","instrumentID","instrument_type","source_file"
    ]
    # همه‌ی شناسه‌ها را دابل‌کوتیشن کن تا case حفظ شود
    cols_sql = ",".join([f'"{c}"' for c in cols])

    insert_sql = f"""
        INSERT INTO symboldetail
        ({cols_sql})
        VALUES ({",".join(["%s"]*len(cols))})
        ON CONFLICT ("insCode") DO UPDATE SET
            "name"          = EXCLUDED."name",
            "name_en"       = EXCLUDED."name_en",
            "sector"        = EXCLUDED."sector",
            "sector_code"   = EXCLUDED."sector_code",
            "subsector"     = EXCLUDED."subsector",
            "market"        = EXCLUDED."market",
            "panel"         = EXCLUDED."panel",
            "stock_ticker"  = EXCLUDED."stock_ticker",
            "share_number"  = EXCLUDED."share_number",
            "base_vol"      = EXCLUDED."base_vol",
            "instrumentID"  = EXCLUDED."instrumentID",
            "instrument_type" = EXCLUDED."instrument_type",
            "source_file"   = EXCLUDED."source_file";
    """

    values = [[r.get(c) for c in cols] for r in rows]

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, values, page_size=500)
        conn.commit()

def main():
    id_type_pairs = read_ids_with_type()
    out, failed = [], []
    total = len(id_type_pairs)

    for i, (ins, inst_type, filename) in enumerate(id_type_pairs, 1):
        try:
            info = fetch_info(ins)
            if not info.get("insCode"):
                raise ValueError("empty insCode from API")
            info["instrument_type"] = inst_type
            info["source_file"] = filename
            out.append(info)
            # 👇 پرینت پیشرفت
            print(f"[{i}/{total}] ✅ دریافت شد: {ins} ({inst_type})")

        except Exception as e:
            failed.append((ins, inst_type, str(e)))
            print(f"❌ {ins} ({inst_type}) : {e}")
        finally:
            time.sleep(0)

        if out and len(out) % 100 == 0:
            print(f"💾 ذخیره 100 رکورد تا الان ...")
            upsert_symboldetail(out)
            out.clear()

    if out:
        print(f"💾 ذخیره {len(out)} رکورد باقی‌مانده ...")
        upsert_symboldetail(out)

    print(f"✅ Done. saved={len(id_type_pairs)-len(failed)} failed={len(failed)}")
    if failed:
        tmp = tempfile.gettempdir()
        fail_path = os.path.join(tmp, "symboldetail_failed.csv")
        with open(fail_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["insCode","instrument_type","error"]); w.writerows(failed)
        print(f"⚠️ failed list: {fail_path}")

if __name__ == "__main__":
    main()