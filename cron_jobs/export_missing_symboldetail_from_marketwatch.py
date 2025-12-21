# -*- coding: utf-8 -*-
import os
import re
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Set, Optional, Tuple

import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env (optional)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------
# Settings
# ---------------------------

MARKETWATCH_URL = "http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx"
INSTRUMENT_INFO_URL = "https://cdn.tsetmc.com/api/Instrument/GetInstrumentInfo/{inscode}"

TIMEOUT = 45
MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 0.3
VERIFY_SSL = True

# مثل FinPy: فقط این بازارها
ALLOWED_MKT_IDS = {"300", "303", "305", "309", "400", "403", "404"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ---------------------------
# DB URL helper (sync)
# ---------------------------

def get_sync_db_url() -> str:
    """
    Priority:
      1) DB_URL_SYNC
      2) DB_URL (convert asyncpg -> psycopg2)
    """
    db_url_sync = os.getenv("DB_URL_SYNC")
    if db_url_sync:
        if db_url_sync.startswith("postgresql://"):
            return db_url_sync.replace("postgresql://", "postgresql+psycopg2://", 1)
        if db_url_sync.startswith("postgresql+asyncpg://"):
            return db_url_sync.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
        return db_url_sync

    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Neither DB_URL_SYNC nor DB_URL is set in .env")

    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return db_url


def get_engine() -> Engine:
    db = get_sync_db_url()
    return create_engine(db, pool_pre_ping=True)

# ---------------------------
# Common helpers
# ---------------------------

def _safe_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return default
        return int(float(s))
    except Exception:
        return default


def _requests_get(url: str) -> requests.Response:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT, verify=VERIFY_SSL)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            logging.warning(f"Attempt {attempt}/{MAX_RETRIES} failed: {url} | {e}")
            time.sleep(0.7 * attempt)
    raise RuntimeError(f"Request failed after {MAX_RETRIES} retries: {url} | last_err={last_err}")

# ---------------------------
# 1) Extract traded insCodes from MarketWatchPlus (Value > 0)
# ---------------------------

def fetch_traded_inscodes_from_marketwatchplus() -> Tuple[Set[int], pd.DataFrame]:
    r = _requests_get(MARKETWATCH_URL)
    main_text = r.text

    parts = main_text.split("@")
    if len(parts) < 4:
        raise RuntimeError("MarketWatchPlus content unexpected: split('@') < 4")

    table_txt = parts[2]
    rows = [x for x in table_txt.split(";") if x.strip()]
    parsed = [row.split(",") for row in rows]
    parsed = [p for p in parsed if len(p) >= 23]
    if not parsed:
        raise RuntimeError("No rows parsed from MarketWatchPlus table section.")

    df = pd.DataFrame(parsed).iloc[:, :23].copy()
    df.columns = [
        "WEB-ID","Ticker-Code","Ticker","Name","Time","Open","Final","Close","No","Volume","Value",
        "Low","High","Y-Final","EPS","Base-Vol","Unknown1","Unknown2","Sector","Day_UL","Day_LL","Share-No","Mkt-ID"
    ]

    df = df[df["Mkt-ID"].astype(str).isin(ALLOWED_MKT_IDS)].copy()
    df["Value_int"] = df["Value"].apply(_safe_int)

    traded_df = df[df["Value_int"] > 0].copy()
    traded_df["insCode"] = traded_df["WEB-ID"].apply(_safe_int)

    traded_ids = set(int(x) for x in traded_df["insCode"].tolist() if int(x) > 0)
    return traded_ids, traded_df

# ---------------------------
# 2) Load existing insCodes from DB (symboldetail/symbolDetail)
# ---------------------------

def fetch_table_columns(engine: Engine, table_name: str, schema: str = "public") -> List[str]:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """)
    with engine.begin() as conn:
        rows = conn.execute(q, {"schema": schema, "table": table_name}).fetchall()
    return [r[0] for r in rows]


def pick_first_existing(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def find_existing_table_name(engine: Engine, candidates: List[str], schema: str = "public") -> str:
    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = ANY(:candidates)
        ORDER BY table_name
        LIMIT 1
    """)
    with engine.begin() as conn:
        r = conn.execute(q, {"schema": schema, "candidates": candidates}).fetchone()
    if not r:
        raise RuntimeError(f"None of these tables exist in schema={schema}: {candidates}")
    return r[0]


def load_existing_inscodes_from_db(engine: Engine) -> Set[int]:
    table_name = find_existing_table_name(engine, ["symboldetail", "symbolDetail"], "public")
    cols = fetch_table_columns(engine, table_name, "public")

    ins_col = pick_first_existing(cols, ["insCode", "inscode", "ins_code"])
    if not ins_col:
        raise RuntimeError(f"Could not find insCode-like column in {table_name}. cols={cols}")

    # فقط insCode
    q = text(f'SELECT "{ins_col}" AS "insCode" FROM "{table_name}" WHERE "{ins_col}" IS NOT NULL')
    with engine.begin() as conn:
        rows = conn.execute(q).fetchall()

    s: Set[int] = set()
    for (v,) in rows:
        iv = _safe_int(v, default=0)
        if iv > 0:
            s.add(iv)
    return s

# ---------------------------
# 3) Fetch instrument info for missing ids
# ---------------------------

def fetch_instrument_info(inscode: int) -> Dict[str, Any]:
    url = INSTRUMENT_INFO_URL.format(inscode=inscode)
    r = _requests_get(url)
    js = r.json() or {}
    info = (js.get("instrumentInfo") or {}) or {}
    sector = (info.get("sector") or {}) or {}

    return {
        "insCode": info.get("insCode"),
        "name": info.get("lVal30"),
        "name_en": info.get("lVal18"),
        "sector": sector.get("lSecVal"),
        "sector_code": sector.get("cSecVal"),
        "subsector": info.get("faraDesc"),
        "market": info.get("flowTitle"),
        "panel": info.get("cgrValCotTitle"),
        "stock_ticker": info.get("lVal18AFC"),
        "share_number": info.get("zTitad"),
        "base_vol": info.get("baseVol"),
        "instrumentID": info.get("instrumentID"),
    }

# ---------------------------
# 4) Export to Excel (no DB writes)
# ---------------------------

def export_missing_to_excel_from_db(
    out_xlsx: str = "missing_in_symboldetail.xlsx",
    fetch_details: bool = True
):
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))

    # (A) traded ids from MW+
    traded_ids, traded_df = fetch_traded_inscodes_from_marketwatchplus()
    logging.info(f"MarketWatchPlus: traded_ids(Value>0) = {len(traded_ids)}")

    # (B) existing ids from DB
    existing_ids = load_existing_inscodes_from_db(engine)
    logging.info(f"Loaded existing insCodes from DB symboldetail: {len(existing_ids)}")

    # (C) diff
    missing_ids = sorted(list(traded_ids - existing_ids))
    logging.info(f"Missing in symboldetail (traded - existing) = {len(missing_ids)}")

    summary = pd.DataFrame([{
        "started_at": started_at,
        "marketwatch_traded_count": len(traded_ids),
        "existing_count": len(existing_ids),
        "missing_count": len(missing_ids),
    }])

    missing_basic = pd.DataFrame({"insCode": missing_ids})

    details_rows: List[Dict[str, Any]] = []
    failed_rows: List[Dict[str, Any]] = []

    if fetch_details and missing_ids:
        total = len(missing_ids)
        for i, ins in enumerate(missing_ids, 1):
            logging.info(f"[{i}/{total}] fetch_info: {ins}")
            try:
                info = fetch_instrument_info(ins)
                if not info.get("insCode"):
                    raise ValueError("empty insCode from API")
                info["source"] = "MarketWatchPlus(Value>0)"
                details_rows.append(info)
            except Exception as e:
                failed_rows.append({"insCode": ins, "error": str(e)})
            finally:
                time.sleep(SLEEP_BETWEEN_REQUESTS)

    details_df = pd.DataFrame(details_rows)
    failed_df = pd.DataFrame(failed_rows)

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="summary")
        missing_basic.to_excel(writer, index=False, sheet_name="missing_ids")
        traded_df.to_excel(writer, index=False, sheet_name="marketwatch_traded_rows")
        if fetch_details:
            details_df.to_excel(writer, index=False, sheet_name="missing_details")
            failed_df.to_excel(writer, index=False, sheet_name="failed_details")

    logging.info(f"✅ Excel saved: {os.path.abspath(out_xlsx)}")


if __name__ == "__main__":
    export_missing_to_excel_from_db(
        out_xlsx="missing_in_symboldetail.xlsx",
        fetch_details=True
    )
