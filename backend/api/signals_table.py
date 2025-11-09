# -*- coding: utf-8 -*-
from enum import Enum
from typing import Optional, Any, List, Dict
import math

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.logger import logger


router = APIRouter(prefix="/signals", tags=["📋 Signals Table"])


# ---------------- Enums ----------------
class PeriodEnum(str, Enum):
    daily = "daily"
    weekly = "weekly"


class CurrencyEnum(str, Enum):
    rial = "rial"
    usd = "usd"


class ViewEnum(str, Enum):
    indicator = "indicator"   # نمایش بر اساس اندیکاتور
    industry = "industry"     # نمایش بر اساس صنعت


class IndiEnum(str, Enum):
    macd = "macd"
    rsi = "rsi"
    ema = "ema"


# ---------- Utils ----------
async def get_existing_cols(db: AsyncSession, table: str) -> set:
    """بررسی نام ستون‌های موجود در جدول (lower-case)"""
    q = text("""
        SELECT lower(column_name)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(:t)
    """)
    res = await db.execute(q, {"t": table})
    return {r[0] for r in res.fetchall()}


def pick_first_exist(candidates: List[str], exists: set, *, required: bool = False) -> Optional[str]:
    """اولین نام ستونی که در جدول هست را برمی‌گرداند (case-insensitive)"""
    for c in candidates:
        if c.lower() in exists:
            return c
    if required:
        raise KeyError(f"None of candidates exist: {candidates}")
    return None


# ---------- SQL-safe helpers ----------
# توجه: مشکل شما از mismatch نوع‌ها در CASE بود؛ در این نسخه هر دو شاخه را داخل CASE به float8 می‌بریم.

_NUMERIC_REGEX = r"^\s*[+-]?(\d+(\.\d+)?|\.\d+)\s*$"

def sql_is_bad(col_sql: str) -> str:
    """تشخیص NaN/±Inf با cast به text"""
    return f"(({col_sql})::text IN ('NaN','Infinity','-Infinity'))"

def sql_is_numeric_str(col_sql: str) -> str:
    """چک می‌کند مقدار متنی شبیه عدد است (برای ستون‌های متنی)"""
    return f"(({col_sql})::text ~ '{_NUMERIC_REGEX}')"

def sql_safe_null_numeric(col_sql: str) -> str:
    """
    خروجی: float8
    اگر NULL/NaN/Inf → NULL::float8
    اگر متن عددی → ::float8
    اگر متن غیرعددی/بولین/هرچیز دیگر → NULL::float8 (برای جلوگیری از خطای cast)
    """
    return (
        f"(CASE "
        f"  WHEN {col_sql} IS NULL OR {sql_is_bad(col_sql)} THEN NULL::float8 "
        f"  WHEN {sql_is_numeric_str(col_sql)} THEN ({col_sql})::float8 "
        f"  WHEN pg_typeof({col_sql})::text IN ('double precision','numeric','real','integer','bigint','smallint') THEN ({col_sql})::float8 "
        f"  ELSE NULL::float8 "
        f"END)"
    )

def sql_safe_zero_numeric(col_sql: str) -> str:
    """
    خروجی: float8
    اگر NULL/NaN/Inf → 0::float8
    اگر متن عددی → ::float8
    اگر متن غیرعددی/بولین/هرچیز دیگر → 0::float8
    """
    return (
        f"(CASE "
        f"  WHEN {col_sql} IS NULL OR {sql_is_bad(col_sql)} THEN 0::float8 "
        f"  WHEN {sql_is_numeric_str(col_sql)} THEN ({col_sql})::float8 "
        f"  WHEN pg_typeof({col_sql})::text IN ('double precision','numeric','real','integer','bigint','smallint') THEN ({col_sql})::float8 "
        f"  ELSE 0::float8 "
        f"END)"
    )


# ---------- JSON sanitize ----------
def _json_sanitize(val):
    """تبدیل NaN و Inf به None برای JSON-safe خروجی"""
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
    return val


@router.get(
    "/table",
    summary="جدول سیگنال‌ها (Daily/Weekly + Rial/USD + Industry/Indicator)",
)
async def signals_table(
    freq: PeriodEnum = Query(..., description="daily یا weekly"),
    currency: CurrencyEnum = Query(..., description="rial یا usd"),
    view: ViewEnum = Query(..., description="indicator یا industry"),
    indicator: Optional[IndiEnum] = Query(None, description="اگر view=indicator -> macd/rsi/ema"),
    sector: Optional[str] = Query(None, description="اگر view=industry -> نام صنعت"),
    limit: int = Query(200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(require_permissions("Report.Signals.Table", "ALL")),
):
    """
    خروجی:
      - latest_date: آخرین تاریخ موجود (daily: date_miladi / weekly: week_end)
      - rows: لیست ردیف‌ها برای جدول (ستون‌های پایه + سیگنال‌ها + موقعیت ایچی)
      - هیچ محاسبه‌ای درباره‌ی حجم وجود ندارد.
    """
    # 1) جدول و ستون تاریخ
    if freq == PeriodEnum.daily:
        table = "daily_joined_data"
        date_col = "date_miladi"
    else:
        table = "weekly_joined_data"
        date_col = "week_end"

    # 2) ستون‌های موجود
    try:
        existing = await get_existing_cols(db, table)
    except Exception as e:
        logger.exception("❌ DB introspection failed")
        raise HTTPException(status_code=500, detail=f"DB error (introspect columns): {e}")

    # 3) ستون‌های پایه
    try:
        symbol_col = pick_first_exist(["stock_ticker"], existing, required=True)
        name_col   = pick_first_exist(["name", "name_en"], existing, required=True)
        sector_col = pick_first_exist(["sector"], existing, required=True)
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"❌ Required base column missing: {e}")

    # 4) قیمت برای ابر (اول adjust_close(_usd)، در صورت نبود last_price(_usd))
    price_col = "adjust_close_usd" if currency == CurrencyEnum.usd else "adjust_close"
    if price_col.lower() not in existing:
        price_col = "last_price_usd" if currency == CurrencyEnum.usd else "last_price"
        if price_col.lower() not in existing:
            raise HTTPException(status_code=500, detail="❌ No suitable price column found for Ichimoku position")

    # 5) Senkou A/B
    try:
        if freq == PeriodEnum.weekly and currency == CurrencyEnum.usd:
            senkou_a_raw = pick_first_exist(["senkou_a_d", "senkou_a"], existing, required=True)
            senkou_b_raw = pick_first_exist(["senkou_b_d", "senkou_b"], existing, required=True)
        else:
            senkou_a_raw = pick_first_exist(["senkou_a"], existing, required=True)
            senkou_b_raw = pick_first_exist(["senkou_b"], existing, required=True)
    except KeyError as e:
        raise HTTPException(status_code=500, detail=f"❌ Ichimoku columns missing: {e}")

    # نسخه ایمن عددی (داخل CASE به float8)
    price_safe  = sql_safe_null_numeric(f"t.{price_col}")
    senkou_a    = sql_safe_null_numeric(f"t.{senkou_a_raw}")
    senkou_b    = sql_safe_null_numeric(f"t.{senkou_b_raw}")

    # 6) نگاشت ستون‌های سیگنال (ایمن و float8)
    def map_sig(src_candidates: List[str], alias: str) -> Optional[str]:
        col = pick_first_exist(src_candidates, existing, required=False)
        if not col:
            return None
        return f"{sql_safe_zero_numeric(f't.{col}')} AS {alias}"

    if freq == PeriodEnum.daily and currency == CurrencyEnum.usd:
        sig_defs = [
            map_sig(["signal_ichimoku_buy_usd", "signal_ichimouku_buy_usd"], "sig_ich_buy"),
            map_sig(["signal_ichimoku_sell_usd", "signal_ichimouku_sell_usd"], "sig_ich_sell"),
            map_sig(["signal_ema_cross_buy_usd"], "sig_ema_buy"),
            map_sig(["signal_ema_cross_sell_usd"], "sig_ema_sell"),
            map_sig(["signal_rsi_buy_usd"], "sig_rsi_buy"),
            map_sig(["signal_rsi_sell_usd"], "sig_rsi_sell"),
            map_sig(["signal_macd_buy_usd"], "sig_macd_buy"),
            map_sig(["signal_macd_sell_usd"], "sig_macd_sell"),
            map_sig(["signal_ema50_100_buy_usd", "signal_ema_50_100_buy_usd"], "sig_ema50100_buy"),
            map_sig(["signal_ema50_100_sell_usd", "signal_ema_50_100_sell_usd"], "sig_ema50100_sell"),
            map_sig(["renko_22_usd"], "renko"),
        ]
    elif freq == PeriodEnum.daily and currency == CurrencyEnum.rial:
        sig_defs = [
            map_sig(["signal_ichimoku_buy", "signal_ichimouku_buy"], "sig_ich_buy"),
            map_sig(["signal_ichimoku_sell", "signal_ichimouku_sell"], "sig_ich_sell"),
            map_sig(["signal_ema_cross_buy"], "sig_ema_buy"),
            map_sig(["signal_ema_cross_sell"], "sig_ema_sell"),
            map_sig(["signal_rsi_buy"], "sig_rsi_buy"),
            map_sig(["signal_rsi_sell"], "sig_rsi_sell"),
            map_sig(["signal_macd_buy"], "sig_macd_buy"),
            map_sig(["signal_macd_sell"], "sig_macd_sell"),
            map_sig(["signal_ema50_100_buy", "signal_ema_50_100_buy"], "sig_ema50100_buy"),
            map_sig(["signal_ema50_100_sell", "signal_ema_50_100_sell"], "sig_ema50100_sell"),
            map_sig(["renko_22"], "renko"),
        ]
    elif freq == PeriodEnum.weekly and currency == CurrencyEnum.rial:
        sig_defs = [
            map_sig(["signal_ichimoku_buy", "signal_ichimouku_buy"], "sig_ich_buy"),
            map_sig(["signal_ichimoku_sell", "signal_ichimouku_sell"], "sig_ich_sell"),
            map_sig(["signal_ema_cross_buy"], "sig_ema_buy"),
            map_sig(["signal_ema_cross_sell"], "sig_ema_sell"),
            map_sig(["signal_rsi_buy"], "sig_rsi_buy"),
            map_sig(["signal_rsi_sell"], "sig_rsi_sell"),
            map_sig(["signal_macd_buy"], "sig_macd_buy"),
            map_sig(["signal_macd_sell"], "sig_macd_sell"),
            map_sig(["signal_ema50_100_buy", "signal_ema_50_100_buy"], "sig_ema50100_buy"),
            map_sig(["signal_ema50_100_sell", "signal_ema_50_100_sell"], "sig_ema50100_sell"),
            map_sig(["renko_52"], "renko"),
        ]
    else:  # weekly + usd (D-suffixed)
        sig_defs = [
            map_sig(["signal_ichimoku_buy_d", "signal_ichimouku_buy_d"], "sig_ich_buy"),
            map_sig(["signal_ichimoku_sell_d", "signal_ichimouku_sell_d"], "sig_ich_sell"),
            map_sig(["signal_ema_cross_buy_d"], "sig_ema_buy"),
            map_sig(["signal_ema_cross_sell_d"], "sig_ema_sell"),
            map_sig(["signal_rsi_buy_d"], "sig_rsi_buy"),
            map_sig(["signal_rsi_sell_d"], "sig_rsi_sell"),
            map_sig(["signal_macd_buy_d"], "sig_macd_buy"),
            map_sig(["signal_macd_sell_d"], "sig_macd_sell"),
            map_sig(["signal_ema50_100_buy_d", "signal_ema_50_100_buy_d"], "sig_ema50100_buy"),
            map_sig(["signal_ema50_100_sell_d", "signal_ema_50_100_sell_d"], "sig_ema50100_sell"),
            map_sig(["renko_52_d"], "renko"),
        ]

    sig_select_parts = [s for s in sig_defs if s is not None]
    sig_select = ",\n            ".join(sig_select_parts)
    if not sig_select:
        raise HTTPException(status_code=500, detail="❌ No signal columns found for the selected mode.")

    # 8) آخرین تاریخ
    try:
        res_max = await db.execute(text(f"SELECT MAX({date_col}) FROM {table}"))
        latest_date = res_max.scalar_one()
        if latest_date is None:
            return {
                "status": "success",
                "params": {"freq": freq, "currency": currency, "view": view, "indicator": indicator, "sector": sector},
                "latest_date": None,
                "rows": [],
                "message": "No data.",
            }
    except Exception as e:
        logger.exception("❌ max(date) failed")
        raise HTTPException(status_code=500, detail=f"DB error (max date): {e}")

    # 9) SELECT base + date (همه به float8 ایمن شده‌اند)
    base_select = f"""
        t.{symbol_col} AS symbol,
        COALESCE(t.{name_col}, t.{name_col}) AS security_name,
        t.{sector_col} AS sector,
        {price_safe} AS price,
        t.{date_col} AS date
    """

    # 10) Ichimoku position (فقط همین—هیچ حجم/AVG)
    ich_position = f"""
        CASE
            WHEN {price_safe} IS NULL OR {senkou_a} IS NULL OR {senkou_b} IS NULL THEN NULL
            WHEN {price_safe} > GREATEST({senkou_a}, {senkou_b}) THEN 'Above Cloud'
            WHEN {price_safe} < LEAST({senkou_a}, {senkou_b}) THEN 'Below Cloud'
            ELSE 'Inside Cloud'
        END AS ich_position
    """

    # 11) فیلتر سطح اول (فقط تاریخ و در صورت industry، خود صنعت)
    where_first = "WHERE t.{date_col} = :latest".format(date_col=date_col)
    params: Dict[str, Any] = {"latest": latest_date, "limit": limit}
    if view == ViewEnum.industry and sector:
        where_first += f" AND t.{sector_col} = :sector "
        params["sector"] = sector

    # 12) CTE پایه (بدون حجم)
    select_parts = [base_select, sig_select, ich_position]
    cte_sql = f"""
        WITH base AS (
            SELECT
                {" ,\n                ".join(select_parts)}
            FROM {table} t
            {where_first}
        )
    """

    # 13) فیلتر لایه دوم (روی aliasها)
    where_second = ""
    if view == ViewEnum.indicator:
        if indicator == IndiEnum.macd:
            where_second = (
                "WHERE (COALESCE(sig_macd_buy,0) <> 0) OR (COALESCE(sig_macd_sell,0) <> 0)"
            )
        elif indicator == IndiEnum.rsi:
            where_second = (
                "WHERE (COALESCE(sig_rsi_buy,0) <> 0) OR (COALESCE(sig_rsi_sell,0) <> 0)"
            )
        elif indicator == IndiEnum.ema:
            where_second = (
                "WHERE (COALESCE(sig_ema_buy,0) <> 0) OR (COALESCE(sig_ema_sell,0) <> 0) "
                "   OR (COALESCE(sig_ema50100_buy,0) <> 0) OR (COALESCE(sig_ema50100_sell,0) <> 0)"
            )

    # 14) SELECT نهایی
    final_sql = f"""
        {cte_sql}
        SELECT *
        FROM base
        {where_second}
        ORDER BY sector, symbol
        LIMIT :limit
    """

    # 15) اجرا
    try:
        cur = await db.execute(text(final_sql), params)
        rows = [dict(r._mapping) for r in cur.fetchall()]
        rows = [{k: _json_sanitize(v) for k, v in row.items()} for row in rows]
    except Exception as e:
        logger.exception("❌ SELECT signals failed")
        raise HTTPException(status_code=500, detail=f"DB error (select rows): {e}")

    return {
        "status": "success",
        "params": {
            "freq": freq,
            "currency": currency,
            "view": view,
            "indicator": indicator,
            "sector": sector
        },
        "latest_date": str(latest_date) if latest_date is not None else None,
        "rows": rows,
        "message": "OK",
    }
