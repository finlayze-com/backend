# -*- coding: utf-8 -*-
"""
Indicator Table Report API
- منبع دیتا: daily_joined_data / weekly_joined_data (با پارامتر mode)
- فیلتر: sector (یا industry)
- سورت: بر اساس اندیکاتورها/ستون‌های انتخابی
- خروجی: شِمای ستون‌ها مطابق جدول نمایشی (قابل تنظیم با COLUMN_MAP)
"""

from enum import Enum
from typing import Literal, Sequence

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response


router = APIRouter(prefix="/report", tags=["📈 Indicator Report"])


# ---- انتخاب جدول: daily یا weekly
class Mode(str, Enum):
    daily = "daily"
    weekly = "weekly"


# ---- مپِ نام ستون‌ها (اگر در ویوهای شما فرق دارد، فقط اینجا را ادیت کنید)
# نکته: ستون‌ها را تا جای ممکن جنریک انتخاب کردم. اگر نام‌ها متفاوت است، alias ها را
# با نام واقعی ستون‌هایتان جایگزین کنید.
COLUMN_MAP = {
    # مشترک
    "sector": "sector",                 # یا sector_name
    "symbol": "stock_ticker",             # یا symbol
    "security_name": "security_name",     # یا name_fa / name_en

    # اندیکاتورها/ویژگی‌ها (مطابق تصویر)
    "macd": "macd_trend",                 # Bullish/Bearish/expect...
    "rsi": "rsi_trend",                   # Bullish/Bearish/...
    "ema20": "ema20_change_pct",          # % تغییر 20
    "ema50": "ema50_change_pct",          # % تغییر 50
    "ema100": "ema100_change_pct",        # % تغییر 100

    "sig1": "signal_1",                   # سیگنال نوع یک
    "sig2": "signal_2",                   # سیگنال نوع دو
    "sig3": "signal_3",                   # سیگنال نوع سه

    "ich_up": "ichimoku_cloud_up",        # ابر بالا
    "ich_dn": "ichimoku_cloud_down",      # ابر پایین
    "ich_kijun": "ichimoku_kijun",        # کیجون

    "price": "last_price",                # قیمت
    "signal_volume": "signal_volume",     # حجم سیگنال (ستون آخر تصویر)
}

# ستون‌های مجاز برای sort
ALLOWED_SORT = {
    "sector", "symbol", "security_name",
    "macd", "rsi", "ema20", "ema50", "ema100",
    "sig1", "sig2", "sig3",
    "ich_up", "ich_dn", "ich_kijun",
    "price", "signal_volume"
}


def table_name_for_mode(mode: Mode) -> str:
    return "daily_joined_data" if mode == Mode.daily else "weekly_joined_data"


@router.get(
    "/indicator-table",
    summary="جدول سیگنال‌ها/اندیکاتورها برای نمادهای یک صنعت (daily/weekly)",
)
async def get_indicator_table(
    mode: Mode = Query(Mode.daily, description="daily یا weekly"),
    sector: str | None = Query(
        None,
        description="نام صنعت/سکتور برای فیلتر (مثلاً: بانک‌ها). اگر None باشد همه‌ی صنایع برمی‌گردد.",
    ),
    sectors: Sequence[str] | None = Query(
        None,
        description="لیست چندگانه از صنایع برای فیلتر (در صورت نیاز).",
    ),
    search: str | None = Query(
        None,
        description="جست‌وجو داخل نام نماد یا نام شرکت (ILIKE)."
    ),
    sort_by: str = Query(
        "signal_volume",
        description=f"ستون سورت. گزینه‌ها: {', '.join(sorted(ALLOWED_SORT))}"
    ),
    order: Literal["asc", "desc"] = Query("desc", description="asc یا desc"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Indicator.Table", "ALL")),
):
    # اعتبارسنجی sort
    if sort_by not in ALLOWED_SORT:
        raise HTTPException(status_code=400, detail=f"sort_by نامعتبر است. مجاز: {sorted(ALLOWED_SORT)}")

    # نام جدول
    table = table_name_for_mode(mode)

    # نام ستون دیتابیس متناظر با sort_by
    sort_col = COLUMN_MAP[sort_by]

    # ساخت SELECT با alias استاندارد برای فرانت
    # COALESCE برای نال‌ها تا فرانت دردسر نداشته باشد.
    select_cols = f"""
        COALESCE({COLUMN_MAP['sector']}, '')          AS sector,
        COALESCE({COLUMN_MAP['symbol']}, '')          AS symbol,
        COALESCE({COLUMN_MAP['security_name']}, '')   AS security_name,

        COALESCE({COLUMN_MAP['macd']}, '')            AS macd,
        COALESCE({COLUMN_MAP['rsi']}, '')             AS rsi,

        {COLUMN_MAP['ema20']}  AS ema20,
        {COLUMN_MAP['ema50']}  AS ema50,
        {COLUMN_MAP['ema100']} AS ema100,

        COALESCE({COLUMN_MAP['sig1']}, '')            AS sig1,
        COALESCE({COLUMN_MAP['sig2']}, '')            AS sig2,
        COALESCE({COLUMN_MAP['sig3']}, '')            AS sig3,

        {COLUMN_MAP['ich_up']}    AS ich_up,
        {COLUMN_MAP['ich_dn']}    AS ich_dn,
        {COLUMN_MAP['ich_kijun']} AS ich_kijun,

        {COLUMN_MAP['price']}         AS price,
        {COLUMN_MAP['signal_volume']} AS signal_volume
    """

    # فیلترها
    where_clauses = ["1=1"]
    params: dict = {}

    if sector:
        where_clauses.append(f"{COLUMN_MAP['sector']} = :sector")
        params["sector"] = sector

    if sectors:
        where_clauses.append(f"{COLUMN_MAP['sector']} = ANY(:sectors)")
        params["sectors"] = list(sectors)

    if search:
        where_clauses.append(f"({COLUMN_MAP['symbol']} ILIKE :q OR {COLUMN_MAP['security_name']} ILIKE :q)")
        params["q"] = f"%{search}%"

    where_sql = " AND ".join(where_clauses)

    # ORDER BY ایمن (نام ستون از whitelist می‌آید)
    order_sql = "ASC" if order.lower() == "asc" else "DESC"

    sql = f"""
        SELECT
            {select_cols}
        FROM {table}
        WHERE {where_sql}
        ORDER BY {sort_col} {order_sql}, {COLUMN_MAP['symbol']} ASC
        LIMIT :limit OFFSET :offset
    """

    params["limit"] = limit
    params["offset"] = offset

    rows = (await db.execute(text(sql), params)).mappings().all()

    # شمارش کل برای pagination
    count_sql = f"SELECT COUNT(*) AS n FROM {table} WHERE {where_sql}"
    total = (await db.execute(text(count_sql), params)).scalar_one()

    return create_response(
        data={
            "mode": mode.value,
            "items": [dict(r) for r in rows],
            "pagination": {"total": total, "limit": limit, "offset": offset},
        },
        message="OK",
        status_code=200,
    )
