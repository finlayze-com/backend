import os
import time
import requests
from datetime import datetime, date
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch


# =========================
# .env loading مثل کد قبلی
# =========================
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)
DB_URL = os.getenv("DB_URL_SYNC")

if not DB_URL:
    raise ValueError("DB_URL_SYNC not found in .env")


API_URL = "https://cdn.tsetmc.com/api/Instrument/GetInstrumentOptionByInstrumentID/{instrument_id}"


def parse_yyyymmdd_to_date(value):
    """
    تبدیل 20260617 -> date(2026, 6, 17)
    """
    if value is None:
        return None

    value_str = str(value).strip()
    if not value_str or value_str == "0":
        return None

    try:
        return datetime.strptime(value_str, "%Y%m%d").date()
    except Exception:
        return None


def is_option_active(end_date_obj):
    """
    اگر هنوز به سررسید نرسیده/رسیده باشد true
    """
    if end_date_obj is None:
        return False
    return end_date_obj >= date.today()


def fetch_option_data(instrument_id: str):
    url = API_URL.format(instrument_id=instrument_id)

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        return data.get("instrumentOption")
    except Exception as e:
        print(f"[ERROR] fetch failed for instrument_id={instrument_id} | {e}")
        return None


def get_option_rows_from_symboldetail():
    query = """
        SELECT
            "insCode",
            "instrumentID",
            "name",
            "name_en",
            "sector",
            "sector_code",
            "subsector",
            "stock_ticker"
        FROM public.symboldetail
        WHERE source_file = 'option.txt'
          AND "instrumentID" IS NOT NULL
    """

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows_raw = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

    rows = [dict(zip(colnames, row)) for row in rows_raw]
    return rows


def upsert_option_detail(batch_rows):
    if not batch_rows:
        return

    cols = [
        "name",
        "name_en",
        "sector",
        "sector_code",
        "subsector",
        "stock_ticker",
        "ins_code",
        "instrument_id",
        "buy_op",
        "sell_op",
        "contract_size",
        "strike_price",
        "ua_ins_code",
        "begin_date",
        "end_date",
        "a_factor",
        "b_factor",
        "c_factor",
        "is_active",
    ]

    insert_sql = f"""
        INSERT INTO option_detail (
            {",".join(cols)},
            created_at,
            updated_at
        )
        VALUES (
            {",".join(["%s"] * len(cols))},
            NOW(),
            NOW()
        )
        ON CONFLICT (ins_code)
        DO UPDATE SET
            name = EXCLUDED.name,
            name_en = EXCLUDED.name_en,
            sector = EXCLUDED.sector,
            sector_code = EXCLUDED.sector_code,
            subsector = EXCLUDED.subsector,
            stock_ticker = EXCLUDED.stock_ticker,
            instrument_id = EXCLUDED.instrument_id,
            buy_op = EXCLUDED.buy_op,
            sell_op = EXCLUDED.sell_op,
            contract_size = EXCLUDED.contract_size,
            strike_price = EXCLUDED.strike_price,
            ua_ins_code = EXCLUDED.ua_ins_code,
            begin_date = EXCLUDED.begin_date,
            end_date = EXCLUDED.end_date,
            a_factor = EXCLUDED.a_factor,
            b_factor = EXCLUDED.b_factor,
            c_factor = EXCLUDED.c_factor,
            is_active = EXCLUDED.is_active,
            updated_at = NOW()
    """

    values = [[row.get(c) for c in cols] for row in batch_rows]

    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(cur, insert_sql, values, page_size=500)
        conn.commit()


def build_option_row(meta_row, api_data):
    begin_date_obj = parse_yyyymmdd_to_date(api_data.get("beginDate"))
    end_date_obj = parse_yyyymmdd_to_date(api_data.get("endDate"))
    active_flag = is_option_active(end_date_obj)

    return {
        "name": meta_row.get("name"),
        "name_en": meta_row.get("name_en"),
        "sector": meta_row.get("sector"),
        "sector_code": meta_row.get("sector_code"),
        "subsector": meta_row.get("subsector"),
        "stock_ticker": meta_row.get("stock_ticker"),
        "ins_code": str(api_data.get("insCode")) if api_data.get("insCode") is not None else None,
        "instrument_id": api_data.get("instrumentID"),
        "buy_op": api_data.get("buyOP"),
        "sell_op": api_data.get("sellOP"),
        "contract_size": api_data.get("contractSize"),
        "strike_price": api_data.get("strikePrice"),
        "ua_ins_code": str(api_data.get("uaInsCode")) if api_data.get("uaInsCode") is not None else None,
        "begin_date": begin_date_obj,
        "end_date": end_date_obj,
        "a_factor": api_data.get("aFactor"),
        "b_factor": api_data.get("bFactor"),
        "c_factor": api_data.get("cFactor"),
        "is_active": active_flag,
    }


def update_option_detail():
    rows = get_option_rows_from_symboldetail()
    total_rows = len(rows)

    print(f"[INFO] total option rows in symboldetail: {total_rows}")

    if total_rows == 0:
        print("[INFO] no option rows found.")
        return

    out = []
    failed = []

    for idx, row in enumerate(rows, start=1):
        instrument_id = row.get("instrumentID")

        if not instrument_id:
            failed.append((row.get("insCode"), "missing instrumentID"))
            print(f"[WARN] skipped row without instrumentID: {row}")
            continue

        api_data = fetch_option_data(instrument_id)
        if not api_data:
            failed.append((instrument_id, "api fetch failed"))
            continue

        try:
            option_row = build_option_row(row, api_data)
            out.append(option_row)
            print(f"[{idx}/{total_rows}] ✅ fetched: {instrument_id}")
        except Exception as e:
            failed.append((instrument_id, str(e)))
            print(f"[ERROR] build row failed for {instrument_id} | {e}")

        if out and len(out) % 100 == 0:
            print("[INFO] saving 100 rows ...")
            upsert_option_detail(out)
            out.clear()

        time.sleep(0.15)

    if out:
        print(f"[INFO] saving remaining {len(out)} rows ...")
        upsert_option_detail(out)

    print(f"[DONE] success={total_rows - len(failed)} | failed={len(failed)}")

    if failed:
        print("[FAILED SAMPLE]")
        for item in failed[:20]:
            print(item)


if __name__ == "__main__":
    update_option_detail()