# backend/api/industry_returns.py
from fastapi import APIRouter, Query, Depends, HTTPException
from typing import Optional, List, Dict, Any
from datetime import date
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger

router = APIRouter(tags=["📊 Industry Analytics"])

@router.get(
    "/analytics/industry-returns",
    summary="بازدهی نمادهای یک صنعت بین دو تاریخ (بر مبنای Close)"
)
async def industry_returns(
    industry: str = Query(..., description="نام صنعت (industry) دقیقاً مطابق symbolDetail"),
    start_date: date = Query(..., description="تاریخ شروع (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="تاریخ پایان (اختیاری؛ در صورت عدم ارسال، آخرین تاریخ موجود انتخاب می‌شود)"),
    db: AsyncSession = Depends(get_db),
    _: None = Depends(require_permissions(["analytics:view"]))
):
    """
    محاسبه بازدهی نمادهای یک صنعت روی قیمت Close بین نزدیک‌ترین تاریخ‌های موجود به start_date و end_date.
    اگر end_date None باشد، آخرین تاریخ موجود انتخاب می‌شود.
    """

    # نزدیک‌ترین تاریخ شروع
    q_nearest_start = text("""
        WITH syms AS (
            SELECT sd.stock_ticker AS symbol
            FROM symboldetail sd
            WHERE sd.industry = :industry
        )
        SELECT d.recdate
        FROM daily_stock_data d
        JOIN syms s ON s.symbol = d.symbol
        ORDER BY ABS(d.recdate - :start_date)
        LIMIT 1;
    """)
    res_s = await db.execute(q_nearest_start, {"industry": industry, "start_date": start_date})
    row_s = res_s.first()
    if not row_s:
        return create_response(
            data={"per_stock": [], "industry_return_eq": None,
                  "resolved_dates": {"start_date": None, "end_date": None}},
            message=f"داده‌ای برای صنعت «{industry}» پیدا نشد.",
            status_code=200
        )
    resolved_start_date = row_s[0]

    # نزدیک‌ترین یا آخرین تاریخ پایان
    if end_date is not None:
        q_nearest_end = text("""
            WITH syms AS (
                SELECT sd.stock_ticker AS symbol
                FROM symboldetail sd
                WHERE sd.industry = :industry
            )
            SELECT d.recdate
            FROM daily_stock_data d
            JOIN syms s ON s.symbol = d.symbol
            ORDER BY ABS(d.recdate - :end_date)
            LIMIT 1;
        """)
        res_e = await db.execute(q_nearest_end, {"industry": industry, "end_date": end_date})
        row_e = res_e.first()
        if row_e:
            resolved_end_date = row_e[0]
        else:
            q_max_end = text("""
                WITH syms AS (
                    SELECT sd.stock_ticker AS symbol
                    FROM symboldetail sd
                    WHERE sd.industry = :industry
                )
                SELECT MAX(d.recdate) AS recdate
                FROM daily_stock_data d
                JOIN syms s ON s.symbol = d.symbol;
            """)
            res_max = await db.execute(q_max_end, {"industry": industry})
            resolved_end_date = res_max.scalar()
    else:
        q_max_end = text("""
            WITH syms AS (
                SELECT sd.stock_ticker AS symbol
                FROM symboldetail sd
                WHERE sd.industry = :industry
            )
            SELECT MAX(d.recdate) AS recdate
            FROM daily_stock_data d
            JOIN syms s ON s.symbol = d.symbol;
        """)
        res_max = await db.execute(q_max_end, {"industry": industry})
        resolved_end_date = res_max.scalar()

    if not resolved_end_date:
        return create_response(
            data={"per_stock": [], "industry_return_eq": None,
                  "resolved_dates": {"start_date": str(resolved_start_date), "end_date": None}},
            message=f"داده‌ای برای تاریخ پایان در صنعت «{industry}» موجود نیست.",
            status_code=200
        )

    if resolved_start_date > resolved_end_date:
        resolved_start_date, resolved_end_date = resolved_end_date, resolved_start_date

    # گرفتن close‌های دو تاریخ
    q_closes = text("""
        WITH syms AS (
            SELECT sd.stock_ticker AS symbol,
                   sd.name,
                   sd.sector,
                   sd.industry
            FROM symboldetail sd
            WHERE sd.industry = :industry
        ),
        s_close AS (
            SELECT d.symbol, d.close AS start_close
            FROM daily_stock_data d
            JOIN syms s ON s.symbol = d.symbol
            WHERE d.recdate = :sdate
        ),
        e_close AS (
            SELECT d.symbol, d.close AS end_close
            FROM daily_stock_data d
            JOIN syms s ON s.symbol = d.symbol
            WHERE d.recdate = :edate
        )
        SELECT s.symbol,
               s.name,
               s.sector,
               s.industry,
               sc.start_close,
               ec.end_close
        FROM syms s
        LEFT JOIN s_close sc ON sc.symbol = s.symbol
        LEFT JOIN e_close ec ON ec.symbol = s.symbol;
    """)

    rows = (await db.execute(
        q_closes,
        {"industry": industry, "sdate": resolved_start_date, "edate": resolved_end_date}
    )).mappings().all()

    per_stock: List[Dict[str, Any]] = []
    for r in rows:
        start_close = r["start_close"]
        end_close = r["end_close"]
        if start_close is None or end_close is None or start_close == 0:
            continue
        ret_pct = ((end_close / start_close) - 1.0) * 100.0
        per_stock.append({
            "symbol": r["symbol"],
            "name": r["name"],
            "sector": r["sector"],
            "industry": r["industry"],
            "start_close": float(start_close),
            "end_close": float(end_close),
            "return_pct": ret_pct
        })

    industry_return_eq = None
    if per_stock:
        industry_return_eq = sum(x["return_pct"] for x in per_stock) / len(per_stock)

    payload = {
        "resolved_dates": {
            "start_date": str(resolved_start_date),
            "end_date": str(resolved_end_date)
        },
        "per_stock": per_stock,
        "industry_return_eq": industry_return_eq
    }

    msg = (
        f"بازدهی نمادهای صنعت «{industry}» بین {resolved_start_date} و {resolved_end_date} محاسبه شد."
        if per_stock else
        f"برای صنعت «{industry}» دیتای کافی برای محاسبه بازدهی در بازه انتخاب‌شده موجود نبود."
    )

    return create_response(data=payload, message=msg, status_code=200)
