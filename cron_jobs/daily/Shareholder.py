# -*- coding: utf-8 -*-
"""
Update shareholders_intervals from TSETMC historical endpoint
- Interval model (SCD-Type-2): start_d_even .. end_d_even
- Only writes when ownership changes; else no-op.
- Fills sector + marketcap(+usd) from daily_joined_data on same or nearest earlier date.

Run examples:
  python -m cron_jobs.daily.Shareholder --from 20251010 --to 20251111 --limit-symbols 1
  python -m cron_jobs.daily.Shareholder --from 20251010 --to 20251111 --concurrency 8
  python -m cron_jobs.daily.Shareholder --all-dates --concurrency 8
"""

import os, asyncio, argparse
from datetime import datetime, timedelta, date as date_cls
from typing import Optional, Dict, Any, List, Tuple

import httpx
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

load_dotenv()
DB_URL = os.getenv("DB_URL")  # postgresql+asyncpg://...
if not DB_URL:
    raise RuntimeError("DB_URL not set in .env")

API_TPL = "https://cdn.tsetmc.com/api/Shareholder/{inscode}/{yyyymmdd}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Finlayze-Shareholders/Intervals/1.0)"}

# آستانه‌ها برای تشخیص تغییر (بدون تغییر منطق)
SHARES_EPS = 1         # حداقل تغییر تعداد سهم
PCT_EPS    = 0.0005    # 0.05bp

# کُدهای تغییر (بدون تغییر)
FLAG_NEW  = 1
FLAG_UP   = 2
FLAG_DOWN = 3


# ---------- تاریخ‌ها ----------
def daterange_yyyymmdd(d_from: str, d_to: str) -> List[str]:
    d1 = datetime.strptime(d_from, "%Y%m%d").date()
    d2 = datetime.strptime(d_to, "%Y%m%d").date()
    days = []
    cur = d1
    while cur <= d2:
        days.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return days

def ymd_to_date(ymd: str) -> date_cls:
    return datetime.strptime(ymd, "%Y%m%d").date()

def ymd_prev(ymd_int: int) -> int:
    d = datetime.strptime(str(ymd_int), "%Y%m%d").date()
    return int((d - timedelta(days=1)).strftime("%Y%m%d"))
# -----------------------------


# ---------- خواندن API (backoff سریع‌تر + http2-friendly) ----------
def _to_str_or_none(x) -> Optional[str]:
    """برخی فیلدها مثل holder_code ممکن است 0 عددی باشند؛ برای asyncpg رشته یا None بفرستیم."""
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and x == 0:
            return None
    except Exception:
        pass
    s = str(x).strip()
    return s or None

def normalize_api_rows(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = js.get("shareShareholder")
    if not isinstance(arr, list):
        return []
    out = []
    seen = set()
    for h in arr:
        rid = h.get("shareHolderShareID")
        key = rid if rid is not None else (h.get("shareHolderName"), h.get("numberOfShares"), h.get("perOfShares"))
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "holder_name": _to_str_or_none(h.get("shareHolderName")),
            "holder_code": _to_str_or_none(h.get("shareHolderID")),  # ممکن است 0 بیاید
            "isin":        _to_str_or_none(h.get("cIsin")),
            "shares":      int(h.get("numberOfShares") or 0),
            "percent":     float(h.get("perOfShares") or 0.0),
        })
    out.sort(key=lambda r: (-(r["percent"] or 0.0), -(r["shares"] or 0)))
    return out

async def fetch_day(client: httpx.AsyncClient, inscode: str, yyyymmdd: str) -> List[Dict[str, Any]]:
    url = API_TPL.format(inscode=inscode, yyyymmdd=yyyymmdd)
    # فقط سرعت: تلاش‌ها 3 بار، backoff کوتاه‌تر. منطق کلی بدون تغییر.
    for attempt in range(3):
        try:
            r = await client.get(url, headers=HEADERS)
            if r.status_code == 200:
                return normalize_api_rows(r.json())
            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            return []
        except Exception:
            await asyncio.sleep(0.5 * (attempt + 1))
    return []
# -------------------------------


# ---------- ابزار کشف ستون‌ها ----------
async def _discover_columns(session: AsyncSession, table: str) -> set:
    q = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name=:tbl
      ORDER BY ordinal_position
    """
    rows = await session.execute(text(q), {"tbl": table})
    return {r[0] for r in rows.all()}

def _pick(colset: set, *cands: str) -> Optional[str]:
    for c in cands:
        if c in colset:
            return c
    return None
# ------------------------------------


# ---------- کوئری‌ها ----------
async def get_symbols(session: AsyncSession, limit_symbols: Optional[int]) -> List[Dict[str, Any]]:
    """
    symboldetail: robust به نام ستون‌ها.
    خروجی: [{inscode, symbol, name, sector}, ...]
    """
    colset = await _discover_columns(session, "symboldetail")

    inscode_col = _pick(colset, "insCode")
    if not inscode_col:
        raise RuntimeError("symboldetail lacks 'insCode' column.")

    symbol_col = _pick(colset, "symbol", "stock_ticker", "Symbol")
    name_col   = _pick(colset, "name", "Name", "company_name", "symbolname")
    sector_col = _pick(colset, "sector", "Sector")

    sel = [
        f'"{inscode_col}"::text AS inscode',
        f'"{symbol_col}" AS symbol' if symbol_col else "NULL::text AS symbol",
        f'"{name_col}"   AS name'   if name_col   else "NULL::text AS name",
        f'"{sector_col}" AS sector' if sector_col else "NULL::text AS sector",
    ]

    sql = f"""
      SELECT {", ".join(sel)}
      FROM symboldetail
      WHERE instrument_type='saham' AND "{inscode_col}" IS NOT NULL
      ORDER BY "{inscode_col}"
    """
    params = {}
    if limit_symbols:
        sql += " LIMIT :lim"
        params["lim"] = limit_symbols

    rows = (await session.execute(text(sql), params)).mappings().all()
    return [dict(r) for r in rows]

async def get_marketcap_and_sector(session: AsyncSession, inscode: str, report_date: date_cls) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """
    خواندن امن (sector, marketcap, marketcap_usd) از daily_joined_data روی همان یا نزدیک‌ترین تاریخ قبل.
    """
    colset = await _discover_columns(session, "daily_joined_data")

    inscode_col  = _pick(colset, "inscode", "insCode")
    date_col     = _pick(colset, "date_miladi", "date", "gdate", "trade_date", "gregorian_date", "tdate", "g_date", "date_g")
    sector_col   = _pick(colset, "sector", "Sector", "industry", "group_name")
    mcap_col     = _pick(colset, "market_cap", "marketcap", "mkt_cap", "marketCap")
    mcap_usd_col = _pick(colset, "market_cap_usd", "marketcap_usd", "mkt_cap_usd", "marketCapUSD")

    if not inscode_col or not date_col:
        return None, None, None

    sel = [
        f'"{sector_col}" AS sector'           if sector_col   else "NULL::text AS sector",
        f'"{mcap_col}" AS marketcap'          if mcap_col     else "NULL::numeric AS marketcap",
        f'"{mcap_usd_col}" AS marketcap_usd'  if mcap_usd_col else "NULL::numeric AS marketcap_usd",
    ]

    sql = f"""
      SELECT {", ".join(sel)}
      FROM daily_joined_data
      WHERE "{inscode_col}"::text = :inscode
        AND "{date_col}" <= :report_date
      ORDER BY "{date_col}" DESC
      LIMIT 1
    """
    row = (await session.execute(text(sql), {"inscode": str(inscode), "report_date": report_date})).mappings().first()
    if not row:
        return None, None, None
    return row["sector"], row["marketcap"], row["marketcap_usd"]

async def get_open_intervals(session: AsyncSession, inscode: str) -> Dict[str, Dict[str, Any]]:
    """
    خواندن رکوردهای باز فعلی (end_d_even IS NULL) برای یک inscode
    خروجی map با کلید holder_name → {shares, percent}
    """
    sql = """
      SELECT holder_name, shares, percent
      FROM shareholders_intervals
      WHERE inscode = :inscode AND end_d_even IS NULL
    """
    rows = (await session.execute(text(sql), {"inscode": str(inscode)})).mappings().all()
    out = {}
    for r in rows:
        nm = r["holder_name"] or ""
        out[nm] = {"shares": int(r["shares"] or 0), "percent": float(r["percent"] or 0.0)}
    return out

async def close_interval(session: AsyncSession, inscode: str, holder_name: str, end_d_even: int):
    """
    بستن رکورد باز فعلی برای این سهامدار (بدون دست‌زدن به change_flag/amount)
    """
    end_date_obj = datetime.strptime(str(end_d_even), "%Y%m%d").date()
    sql = """
      UPDATE shareholders_intervals
      SET end_d_even = :end_d_even,
          end_date   = :end_date
      WHERE inscode = :inscode
        AND holder_name = :holder_name
        AND end_d_even IS NULL
    """
    await session.execute(text(sql), {
        "inscode": str(inscode),
        "holder_name": holder_name,
        "end_d_even": end_d_even,
        "end_date": end_date_obj,
    })

async def open_interval(session: AsyncSession,
                        inscode: str, symbol: Optional[str], name: Optional[str], sector: Optional[str],
                        start_d_even: int,
                        holder: Dict[str, Any],
                        marketcap: Optional[float], marketcap_usd: Optional[float],
                        change_flag: Optional[int], change_amount: Optional[int]):
    """
    ایجاد رکورد جدید (باز کردن interval) + ست‌کردن change_flag/amount روی رکورد جدید
    """
    start_date_obj = datetime.strptime(str(start_d_even), "%Y%m%d").date()

    sql = """
      INSERT INTO shareholders_intervals
      (inscode, symbol, name, sector,
       start_d_even, end_d_even, start_date, end_date,
       d_even, date,
       holder_name, holder_code, isin,
       shares, percent,
       change_flag, change_amount,
       marketcap, marketcap_usd,
       source)
      VALUES
      (:inscode, :symbol, :name, :sector,
       :start_d_even, NULL, :start_date, NULL,
       :start_d_even, :start_date,
       :holder_name, :holder_code, :isin,
       :shares, :percent,
       :change_flag, :change_amount,
       :marketcap, :marketcap_usd,
       'tsetmc_api')
      ON CONFLICT (inscode, holder_name, start_d_even) DO NOTHING
    """
    await session.execute(text(sql), {
        "inscode": str(inscode),
        "symbol": symbol, "name": name, "sector": sector,
        "start_d_even": start_d_even, "start_date": start_date_obj,
        "holder_name": holder["holder_name"],
        "holder_code": holder["holder_code"],
        "isin": holder["isin"],
        "shares": int(holder["shares"] or 0),
        "percent": float(holder["percent"] or 0.0),
        "change_flag": int(change_flag) if change_flag is not None else None,
        "change_amount": int(change_amount) if change_amount is not None else None,
        "marketcap": marketcap,
        "marketcap_usd": marketcap_usd,
    })
# ---------------------------------


# ---------- منطق تغییر ----------
def diff(prev: Dict[str, Any], cur: Dict[str, Any]) -> Tuple[bool, Optional[int], Optional[int]]:
    """
    خروجی:
      (changed?, change_flag, change_amount)
      - change_amount برحسب تغییر سهم (abs(delta shares))
    """
    if prev is None:
        amt = int(cur["shares"] or 0)
        return (True, FLAG_NEW if amt > 0 else FLAG_NEW, amt if amt > 0 else None)

    prev_sh = int(prev.get("shares") or 0)
    prev_pc = float(prev.get("percent") or 0.0)
    cur_sh  = int(cur.get("shares") or 0)
    cur_pc  = float(cur.get("percent") or 0.0)

    ds = abs(cur_sh - prev_sh)
    dp = abs(cur_pc - prev_pc)

    if ds >= SHARES_EPS or dp > PCT_EPS:
        if cur_sh > prev_sh:
            return True, FLAG_UP, (cur_sh - prev_sh)
        elif cur_sh < prev_sh:
            return True, FLAG_DOWN, (prev_sh - cur_sh)
        else:
            return True, FLAG_UP, None
    return False, None, None
# -------------------------------


# ---------- روزهای معاملاتی فقط از DB (افزایش سرعت) ----------
async def trading_days(session: AsyncSession, d_from: str, d_to: str) -> List[str]:
    cols = await _discover_columns(session, "daily_joined_data")
    date_col = _pick(cols, "date_miladi", "date", "gdate", "trade_date", "gregorian_date")
    if not date_col:
        # اگر ستونی پیدا نشد، همان رفتار قبلی (کندتر) را حفظ می‌کنیم
        return daterange_yyyymmdd(d_from, d_to)
    sql = f"""
      SELECT TO_CHAR("{date_col}", 'YYYYMMDD') AS d
      FROM daily_joined_data
      WHERE "{date_col}" BETWEEN TO_DATE(:df, 'YYYYMMDD') AND TO_DATE(:dt, 'YYYYMMDD')
      GROUP BY "{date_col}"
      ORDER BY "{date_col}"
    """
    rows = (await session.execute(text(sql), {"df": d_from, "dt": d_to})).all()
    return [r[0] for r in rows]
# ---------------------------------------------


# ---------- پردازش هر نماد (بهینه‌سازی I/O فقط) ----------
async def process_symbol(session_factory, client, sem, sym: Dict[str, Any], days: List[str]):
    inscode = str(sym["inscode"])
    symbol  = sym.get("symbol")
    name    = sym.get("name")

    # cache ساده برای mcap/sector در هر نماد/روز
    mcap_cache: Dict[str, Tuple[Optional[str], Optional[float], Optional[float]]] = {}

    def _mkey(ymd: str) -> str:
        return f"{inscode}:{ymd}"

    async with sem:
        async with session_factory() as session:
            # 1) open_map فقط یک بار از DB
            open_map = await get_open_intervals(session, inscode)

            for ymd in days:
                rows = await fetch_day(client, inscode, ymd)
                if not rows:
                    continue

                d_even = int(ymd)
                rdate  = ymd_to_date(ymd)

                # 2) mcap/sector با cache
                key = _mkey(ymd)
                if key in mcap_cache:
                    sector_dj, mc, mc_usd = mcap_cache[key]
                else:
                    sector_dj, mc, mc_usd = await get_marketcap_and_sector(session, inscode, rdate)
                    mcap_cache[key] = (sector_dj, mc, mc_usd)

                sector_val = sector_dj if sector_dj else sym.get("sector")

                today_names = {r["holder_name"] or "" for r in rows}

                # 3) برای هر رکورد امروز: فقط از open_map در حافظه بخوان
                for h in rows:
                    nm = h["holder_name"] or ""
                    prev = open_map.get(nm)  # ممکن است None باشد

                    changed, flag, amount = diff(prev, h)
                    if prev:
                        if changed:
                            # بستن رکورد قبلی تا دیروز
                            await close_interval(session, inscode, nm, ymd_prev(d_even))
                            # باز کردن رکورد جدید
                            await open_interval(session, inscode, symbol, name, sector_val,
                                                d_even, h, mc, mc_usd, flag, amount)
                            # به‌روزرسانی in-memory
                            open_map[nm] = {"shares": int(h["shares"] or 0), "percent": float(h["percent"] or 0.0)}
                        # اگر تغییری نبود، هیچ کاری نکن
                    else:
                        # سهامدار جدید
                        await open_interval(session, inscode, symbol, name, sector_val,
                                            d_even, h, mc, mc_usd, FLAG_NEW, h["shares"] or None)
                        open_map[nm] = {"shares": int(h["shares"] or 0), "percent": float(h["percent"] or 0.0)}

                # 4) کسانی که امروز نیستند ولی باز بودند → ببند (و از open_map حذف کن)
                for nm in set(open_map.keys()) - today_names:
                    await close_interval(session, inscode, nm, ymd_prev(d_even))
                    open_map.pop(nm, None)

            await session.commit()
    return inscode
# ----------------------------------


# ---------- محدوده تاریخ خودکار ----------
async def discover_all_dates_range(session_factory) -> Tuple[str, str]:
    """
    از daily_joined_data کوچک‌ترین و بزرگ‌ترین date_miladi را کشف می‌کند.
    """
    async with session_factory() as session:
        colset = await _discover_columns(session, "daily_joined_data")
        date_col = _pick(colset, "date_miladi", "date", "gdate", "trade_date", "gregorian_date")
        if not date_col:
            raise RuntimeError("daily_joined_data lacks a Gregorian date column (e.g., date_miladi).")

        sql = f"""
          SELECT
            TO_CHAR(MIN("{date_col}"), 'YYYYMMDD') AS dmin,
            TO_CHAR(MAX("{date_col}"), 'YYYYMMDD') AS dmax
          FROM daily_joined_data
        """
        row = (await session.execute(text(sql))).mappings().first()
        dmin, dmax = row["dmin"], row["dmax"]
        if not dmin or not dmax:
            raise RuntimeError("Could not discover date range from daily_joined_data.")
        return dmin, dmax
# ---------------------------------------


# ---------- main ----------
async def main_async(date_from: Optional[str], date_to: Optional[str],
                     limit_symbols: Optional[int], concurrency: int, all_dates: bool):
    engine = create_async_engine(DB_URL, pool_pre_ping=True)
    session_factory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    # کشف محدوده تاریخ در صورت نیاز
    if all_dates:
        dmin, dmax = await discover_all_dates_range(session_factory)
        print(f"[date range] discovered from daily_joined_data: {dmin} → {dmax}")
        date_from, date_to = dmin, dmax

    if not date_from or not date_to:
        raise RuntimeError("date_from/date_to is required (or use --all-dates).")

    # دریافت نمادها
    async with session_factory() as session:
        symbols = await get_symbols(session, limit_symbols)
        # روزهای واقعی معامله از DB (اگر نشد، برمی‌گردد به daterange)
        days = await trading_days(session, date_from, date_to)

    sem = asyncio.Semaphore(concurrency)

    # اتصال HTTP بهینه‌شده
    limits = httpx.Limits(max_connections=max(32, concurrency * 4), max_keepalive_connections=max(16, concurrency * 2))
    timeout = httpx.Timeout(12.0)
    async with httpx.AsyncClient(limits=limits, timeout=timeout, http2=True) as client:
        await asyncio.gather(*[
            process_symbol(session_factory, client, sem, sym, days) for sym in symbols
        ])

    await engine.dispose()


def parse_args():
    p = argparse.ArgumentParser(description="Update shareholders_intervals from TSETMC historical API.")
    p.add_argument("--from", dest="date_from", help="YYYYMMDD (Gregorian)")
    p.add_argument("--to", dest="date_to", help="YYYYMMDD (Gregorian)")
    p.add_argument("--limit-symbols", type=int, default=None, help="Limit number of symbols (smoke test)")
    p.add_argument("--concurrency", type=int, default=8, help="Parallel symbols")
    p.add_argument("--all-dates", action="store_true", help="Use full date range from daily_joined_data")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main_async(args.date_from, args.date_to, args.limit_symbols, args.concurrency, args.all_dates))
