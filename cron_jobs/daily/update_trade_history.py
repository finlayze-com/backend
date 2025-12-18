# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import date, timedelta
from typing import Dict, List, Optional, Any

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------
# Settings
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


DB_URL_SYNCED = get_sync_db_url()

BASE_URL = "https://cdn.tsetmc.com/api/Trade/GetTradeHistory"
TIMEOUT = 20
MAX_RETRIES = 3
SLEEP_BETWEEN_REQUESTS = 0.12
BATCH_INSERT_SIZE = 2000
VERIFY_SSL = True
MONTHS_BACK_DEFAULT = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ---------------------------
# DB helpers
# ---------------------------

def get_engine() -> Engine:
    return create_engine(DB_URL_SYNCED, pool_pre_ping=True)


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
    """
    Find the real table name in Postgres (case-sensitive safe).
    """
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


def build_symboldetail_select(engine: Engine) -> str:
    """
    ✅ Fix:
      - table could be symboldetail (lowercase) or "symbolDetail"
      - column could be inscode (lowercase) or insCode
      - aliases MUST be quoted so result keys are exactly:
        insCode, symbol, market, instrument_type, sector
    """
    table_name = find_existing_table_name(engine, candidates=["symboldetail", "symbolDetail"], schema="public")
    cols = fetch_table_columns(engine, table_name, "public")

    ins_col = pick_first_existing(cols, ["insCode", "inscode", "ins_code"])
    if not ins_col:
        raise RuntimeError("Could not find insCode-like column in symboldetail/symbolDetail.")

    # symbol candidates
    symbol_col = pick_first_existing(cols, ["stock_ticker", "symbol", "lVal18AFC", "lval18afc", "lVal30", "lval30"])

    # market candidates (فارسی بازار)
    market_col = pick_first_existing(cols, ["market", "marketTitle", "market_title", "cGrValCotTitle", "marketNameFa"])

    # instrument type candidates (فارسی نوع ابزار)
    inst_type_col = pick_first_existing(cols, ["instrument_type", "instrumentTypeTitle", "cComValTitle", "typeTitle"])

    # sector candidates (فارسی صنعت)
    sector_col = pick_first_existing(cols, ["sector", "industry", "industryTitle", "cSecValTitle", "sectorTitle"])

    def col_or_null(c: Optional[str]) -> str:
        return f"\"{c}\"" if c else "NULL"

    symbol_expr = f"COALESCE(NULLIF(TRIM({col_or_null(symbol_col)}), ''), 'UNKNOWN')"

    # ✅ quoted aliases to force exact mapping keys
    sql = f"""
        SELECT
            "{ins_col}"::bigint AS "insCode",
            {symbol_expr} AS "symbol",
            {col_or_null(market_col)}::text AS "market",
            {col_or_null(inst_type_col)}::text AS "instrument_type",
            {col_or_null(sector_col)}::text AS "sector"
        FROM "{table_name}"
        WHERE "{ins_col}" IS NOT NULL
    """
    return sql


def fetch_symbols(engine: Engine) -> List[Dict[str, Any]]:
    sql = build_symboldetail_select(engine)
    with engine.begin() as conn:
        rows = conn.execute(text(sql)).mappings().all()
    return [dict(r) for r in rows]


def get_max_deven(engine: Engine, inscode: int) -> Optional[int]:
    q = text('SELECT MAX("dEven") AS max_deven FROM trade_history WHERE "insCode" = :insCode')
    with engine.begin() as conn:
        r = conn.execute(q, {"insCode": inscode}).mappings().first()
    if r and r["max_deven"]:
        return int(r["max_deven"])
    return None

# ---------------------------
# Date helpers
# ---------------------------

def yyyymmdd(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


def daterange_inclusive(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

# ---------------------------
# API helpers
# ---------------------------

def request_trade_history(inscode: int, deven: int) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/{inscode}/{deven}/true"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=TIMEOUT, verify=VERIFY_SSL)
            if resp.status_code == 200:
                js = resp.json()
                return js.get("tradeHistory", []) or []
            return []
        except requests.exceptions.SSLError as e:
            logging.error(f"SSL error for {inscode} {deven}: {e}")
            return []
        except Exception as e:
            logging.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {inscode} {deven}: {e}")
            time.sleep(0.7 * attempt)

    return []


def normalize_rows(raw: List[Dict[str, Any]], meta: Dict[str, Any], deven: int) -> List[Dict[str, Any]]:
    out = []
    for r in raw:
        nTran = int(r.get("nTran", 0) or 0)
        if nTran == 0:
            continue

        out.append({
            "insCode": int(meta["insCode"]),
            "symbol": str(meta.get("symbol") or "UNKNOWN"),
            "market": meta.get("market"),
            "instrument_type": meta.get("instrument_type"),
            "sector": meta.get("sector"),
            "dEven": int(deven),
            "nTran": nTran,
            "hEven": int(r.get("hEven", 0) or 0),
            "pTran": float(r.get("pTran", 0.0) or 0.0),
            "qTitTran": int(r.get("qTitTran", 0) or 0),
            "canceled": int(r.get("canceled", 0) or 0),
        })
    return out


def chunked(lst: List[Dict[str, Any]], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def upsert_batch(engine: Engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    q = text("""
        INSERT INTO trade_history
            ("insCode", "symbol", "market", "instrument_type", "sector",
             "dEven", "nTran", "hEven", "pTran", "qTitTran", "canceled")
        VALUES
            (:insCode, :symbol, :market, :instrument_type, :sector,
             :dEven, :nTran, :hEven, :pTran, :qTitTran, :canceled)
        ON CONFLICT ("insCode", "dEven", "nTran")
        DO UPDATE SET
            "symbol" = EXCLUDED."symbol",
            "market" = EXCLUDED."market",
            "instrument_type" = EXCLUDED."instrument_type",
            "sector" = EXCLUDED."sector",
            "hEven" = EXCLUDED."hEven",
            "pTran" = EXCLUDED."pTran",
            "qTitTran" = EXCLUDED."qTitTran",
            "canceled" = EXCLUDED."canceled"
    """)

    with engine.begin() as conn:
        conn.execute(q, rows)

    return len(rows)


# ---------------------------
# Main
# ---------------------------

def run(months_back: int = MONTHS_BACK_DEFAULT, only_today: bool = False):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))

    symbols = fetch_symbols(engine)
    logging.info(f"Loaded {len(symbols)} symbols from symboldetail/symbolDetail.")

    # debug: فقط یک بار اولین meta رو ببین (اگر خواستی کامنت کن)
    if symbols:
        logging.info(f"Meta keys sample: {list(symbols[0].keys())}")

    today = date.today()
    start_backfill = today - timedelta(days=months_back * 30)

    total_rows = 0
    total_requests = 0

    for meta in symbols:
        inscode = int(meta["insCode"])
        sym = meta.get("symbol", "UNKNOWN")
        logging.info(f"→ {sym} ({inscode})")

        if only_today:
            days = [today]
        else:
            max_deven = get_max_deven(engine, inscode)
            start_date = start_backfill

            if max_deven is not None:
                s = str(max_deven)
                if len(s) == 8:
                    last_date = date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
                    start_date = max(start_backfill, last_date + timedelta(days=1))

            if start_date > today:
                continue

            days = list(daterange_inclusive(start_date, today))

        for d in days:
            deven = yyyymmdd(d)
            total_requests += 1

            raw = request_trade_history(inscode, deven)
            time.sleep(SLEEP_BETWEEN_REQUESTS)

            if not raw:
                continue

            rows = normalize_rows(raw, meta, deven)
            if not rows:
                continue

            for b in chunked(rows, BATCH_INSERT_SIZE):
                total_rows += upsert_batch(engine, b)

            logging.info(f"  {deven}: +{len(rows)} rows")

    logging.info(f"Done. requests={total_requests}, inserted/updated={total_rows}")


if __name__ == "__main__":
    run(months_back=6, only_today=False)
