# backend/api/queues_visual.py
# -*- coding: utf-8 -*-

from typing import Optional, Literal, Dict, Any, List
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime, date as dt_date

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions

router = APIRouter(prefix="/queues", tags=["📊 Queues Visuals"])


# --------------------------- Helpers ---------------------------

_EMPTY_DATE_TOKENS = {
    "", "null", "none", "undefined", "invalid", "invalid date", "nan"
}

def _is_empty_like(v: Optional[str]) -> bool:
    """
    هر چیزی که از سمت فرانت ممکنه به عنوان "خالی" بیاد را پوشش می‌دهد:
    None, "", "null", "undefined", "Invalid Date", ...
    """
    if v is None:
        return True
    s = str(v).strip().lower()
    if s in _EMPTY_DATE_TOKENS:
        return True
    if "invalid" in s and "date" in s:
        return True
    return False


def _parse_gregorian_ymd(date_str: str) -> dt_date:
    """
    ورودی: تاریخ میلادی با فرمت YYYY-MM-DD
    خروجی: datetime.date (برای asyncpg باید date واقعی باشد نه str)
    """
    s = (date_str or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="date must be in YYYY-MM-DD format")
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="date must be in YYYY-MM-DD format")


async def _latest_downloaded_day(db: AsyncSession) -> dt_date:
    """
    آخرین روز موجود در quote بر اساس downloaded_at برمی‌گرداند (date واقعی).
    """
    q = text("""SELECT (MAX(q."downloaded_at"))::date AS d FROM quote q""")
    r = await db.execute(q)
    d = r.scalar()
    if not d:
        raise HTTPException(status_code=404, detail="no downloaded_at in quote")

    if isinstance(d, dt_date):
        return d

    # اگر به هر دلیل string شد
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=500, detail="invalid downloaded_at date in DB")


async def _resolve_request_date(date_param: Optional[str], db: AsyncSession) -> dt_date:
    """
    اگر date خالی/نامعتبر بود => آخرین روز downloaded_at
    اگر date معتبر بود => همان (date واقعی)
    """
    if _is_empty_like(date_param):
        return await _latest_downloaded_day(db)
    return _parse_gregorian_ymd(str(date_param))


def _queue_value_case(side: Literal["buy", "sell", "both"]) -> str:
    if side == "buy":
        return 'COALESCE(q."BQ_Value", 0)'
    if side == "sell":
        return 'COALESCE(q."SQ_Value", 0)'
    return 'COALESCE(q."BQ_Value", 0) + COALESCE(q."SQ_Value", 0)'


def _presence_filter(side: Literal["buy", "sell", "both"]) -> str:
    if side == "buy":
        return 'AND COALESCE(q."BQ_Value", 0) > 0'
    if side == "sell":
        return 'AND COALESCE(q."SQ_Value", 0) > 0'
    return 'AND (COALESCE(q."BQ_Value",0) > 0 OR COALESCE(q."SQ_Value",0) > 0)'


# --------------------------- Treemap ---------------------------

@router.get("/treemap", summary="Treemap of queues grouped by sector (ECharts-friendly)")
async def queues_treemap(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (Gregorian)؛ اگر خالی باشد آخرین روز downloaded_at"),
    side: Literal["buy", "sell", "both"] = Query("buy", description="سمت صف برای سایز جعبه‌ها: buy/sell/both"),
    metric: Literal["queue", "base", "value"] = Query(
        "queue",
        description="اندازهٔ جعبه‌ها: queue=ارزش صف، base=base_value، value=ارزش معاملات روز"
    ),
    sector: Optional[str] = Query(None, description="اگر مقدار بدهید فقط همان صنعت برگردانده می‌شود"),
    min_value: Optional[int] = Query(None, description="فیلتر: فقط رکوردهای با مقدار ≥ این عدد"),
    _=Depends(require_permissions("Report.Queues.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    # برای DB باید date واقعی باشد (نه str) تا asyncpg خطا ندهد
    date_db = await _resolve_request_date(date, db)          # dt_date
    date_str = date_db.strftime("%Y-%m-%d")                  # برای response

    qexpr = _queue_value_case(side)
    queue_presence_filter = _presence_filter(side)

    if metric == "queue":
        size_expr = qexpr
    elif metric == "base":
        size_expr = 'COALESCE(q."base_value", 0)'
    else:
        size_expr = 'COALESCE(q."Value", 0)'

    color_expr = '(COALESCE(q."BQ_Value",0) - COALESCE(q."SQ_Value",0))'

    sector_filter_sql = ""
    params: Dict[str, Any] = {"date": date_db}  # 🔥 date واقعی برای پارامتر
    if sector:
        sector_filter_sql = 'AND sd."sector" = :sector'
        params["sector"] = sector

    leaf_sql = f"""
        SELECT
            sd."sector"        AS sector,
            q."stock_ticker"   AS stock_ticker,
            ({size_expr})      AS box_value,
            ({color_expr})     AS color_value
        FROM quote q
        JOIN symboldetail sd
          ON sd."insCode"::text = q."inscode"::text
        WHERE (q."downloaded_at")::date = :date
          AND sd."sector" IS NOT NULL
          {sector_filter_sql}
          {queue_presence_filter}
    """
    res = await db.execute(text(leaf_sql), params)
    rows = res.mappings().all()

    if not rows:
        return {
            "date": date_str,
            "side": side,
            "metric": metric,
            "children": [],
            "color_scale": {"min": 0, "max": 0}
        }

    leaves: List[Dict[str, Any]] = []
    # اگر می‌خوای دقیق‌تر باشه می‌تونی از None شروع کنی، ولی این هم قابل قبوله
    color_min, color_max = 0, 0

    for r in rows:
        v = int(r["box_value"] or 0)
        if v <= 0:
            continue
        if min_value is not None and v < min_value:
            continue

        c = int(r["color_value"] or 0)
        color_min = min(color_min, c)
        color_max = max(color_max, c)

        leaves.append({
            "sector": r["sector"],
            "name": r["stock_ticker"],
            "value": v,
            "color_value": c
        })

    if not leaves:
        return {
            "date": date_str,
            "side": side,
            "metric": metric,
            "children": [],
            "color_scale": {"min": 0, "max": 0}
        }

    sector_bucket: Dict[str, Dict[str, Any]] = {}
    for leaf in leaves:
        sec = leaf["sector"]
        if sec not in sector_bucket:
            sector_bucket[sec] = {"name": sec, "value": 0, "color_value": 0, "children": []}

        sector_bucket[sec]["children"].append({
            "name": leaf["name"],
            "value": leaf["value"],
            "color_value": leaf["color_value"],
        })

        sector_bucket[sec]["value"] += leaf["value"]
        sector_bucket[sec]["color_value"] += leaf["color_value"]

    children = [v for v in sector_bucket.values() if v["value"] > 0]
    children.sort(key=lambda x: x["value"], reverse=True)

    return {
        "date": date_str,
        "side": side,
        "metric": metric,
        "color_scale": {"min": int(color_min), "max": int(color_max)},
        "children": children,
    }


# --------------------------- Bullet ---------------------------

@router.get("/bullet", summary="Bullet chart data: sector stocks or Top-N stocks (buy/sell only)")
async def queues_bullet(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (Gregorian)؛ اگر خالی باشد آخرین روز downloaded_at"),
    scope: Literal["sector", "top"] = Query("sector", description="دامنه محاسبه: sector | top"),
    sector: Optional[str] = Query(None, description="وقتی scope=sector فعال است، نام صنعت (symboldetail.sector)"),
    side: Literal["buy", "sell"] = Query("buy", description="سمت صف برای اندازه measure (فقط buy یا sell)"),
    compare: Literal["base", "value", "both"] = Query("both", description="مقایسه با base_value و/یا day_value"),
    top_n: int = Query(10, ge=1, le=100, description="وقتی scope=top فعال است، تعداد نمادها"),
    _=Depends(require_permissions("Report.Queues.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    date_db = await _resolve_request_date(date, db)
    date_str = date_db.strftime("%Y-%m-%d")

    qexpr = _queue_value_case(side)

    if scope == "sector":
        if not sector:
            raise HTTPException(status_code=400, detail="sector is required when scope=sector")

        sql = f"""
            SELECT
                q."stock_ticker"                                 AS stock,
                SUM(COALESCE(q."BQ_Value", 0))                   AS buy_value_total,
                SUM(COALESCE(q."SQ_Value", 0))                   AS sell_value_total,
                SUM({qexpr})                                     AS queue_value_total,
                SUM(COALESCE(q."base_value", 0))                 AS base_value_total,
                SUM(COALESCE(q."Value", 0))                      AS day_value_total
            FROM quote q
            JOIN symboldetail sd
              ON sd."insCode"::text = q."inscode"::text
            WHERE (q."downloaded_at")::date = :date
              AND sd."sector" = :sector
            GROUP BY q."stock_ticker"
            HAVING SUM(COALESCE(q."BQ_Value", 0)) > 0 OR SUM(COALESCE(q."SQ_Value", 0)) > 0
            ORDER BY queue_value_total DESC
        """
        params = {"date": date_db, "sector": sector}
        res = await db.execute(text(sql), params)
        rows = res.mappings().all()

        items = []
        for r in rows:
            stock            = r["stock"]
            buy_value_total  = int(r["buy_value_total"]  or 0)
            sell_value_total = int(r["sell_value_total"] or 0)
            queue_value_tot  = int(r["queue_value_total"] or 0)
            base_value_total = int(r["base_value_total"]  or 0)
            day_value_total  = int(r["day_value_total"]   or 0)

            range_vs_base  = [0, max(queue_value_tot, base_value_total, 1)]
            range_vs_value = [0, max(queue_value_tot, day_value_total,  1)]

            markers = []
            if compare in ("base", "both"):
                markers.append(base_value_total)
            if compare in ("value", "both"):
                markers.append(day_value_total)

            queue_type = (
                "buy" if buy_value_total > 0
                else "sell" if sell_value_total > 0
                else "none"
            )

            items.append({
                "title": stock,
                "date": date_str,
                "side": side,
                "scope": "stock",
                "compare": compare,
                "measure": queue_value_tot,
                "markers": markers,
                "ranges": {"vs_base": range_vs_base, "vs_value": range_vs_value},
                "raw": {
                    "queue_value_total": queue_value_tot,
                    "base_value_total":  base_value_total,
                    "day_value_total":   day_value_total,
                    "buy_value_total":   buy_value_total,
                    "sell_value_total":  sell_value_total,
                    "queue_type":        queue_type
                }
            })

        return {
            "mode": "sector_stocks",
            "date": date_str,
            "side": side,
            "scope": "sector",
            "sector": sector,
            "compare": compare,
            "count": len(items),
            "items": items
        }

    # scope=top
    sql = f"""
        SELECT
            q."stock_ticker"                AS stock,
            SUM({qexpr})                    AS queue_value_total,
            SUM(COALESCE(q."base_value",0)) AS base_value_total,
            SUM(COALESCE(q."Value",0))      AS day_value_total,
            SUM(COALESCE(q."BQ_Value",0))   AS buy_value_total,
            SUM(COALESCE(q."SQ_Value",0))   AS sell_value_total
        FROM quote q
        WHERE (q."downloaded_at")::date = :date
        GROUP BY q."stock_ticker"
        HAVING SUM(COALESCE(q."BQ_Value", 0)) > 0 OR SUM(COALESCE(q."SQ_Value", 0)) > 0
        ORDER BY queue_value_total DESC
        LIMIT :topn
    """
    params = {"date": date_db, "topn": top_n}
    res = await db.execute(text(sql), params)
    rows = res.mappings().all()

    items = []
    for r in rows:
        stock            = r["stock"]
        queue_value_tot  = int(r["queue_value_total"] or 0)
        base_value_total = int(r["base_value_total"]  or 0)
        day_value_total  = int(r["day_value_total"]   or 0)
        buy_value_total  = int(r["buy_value_total"]   or 0)
        sell_value_total = int(r["sell_value_total"]  or 0)

        range_vs_base  = [0, max(queue_value_tot, base_value_total, 1)]
        range_vs_value = [0, max(queue_value_tot, day_value_total,  1)]

        markers = []
        if compare in ("base", "both"):
            markers.append(base_value_total)
        if compare in ("value", "both"):
            markers.append(day_value_total)

        queue_type = (
            "buy" if buy_value_total > 0
            else "sell" if sell_value_total > 0
            else "none"
        )

        items.append({
            "title": stock,
            "date": date_str,
            "side": side,
            "scope": "stock",
            "compare": compare,
            "measure": queue_value_tot,
            "markers": markers,
            "ranges": {"vs_base": range_vs_base, "vs_value": range_vs_value},
            "raw": {
                "queue_value_total": queue_value_tot,
                "base_value_total":  base_value_total,
                "day_value_total":   day_value_total,
                "buy_value_total":   buy_value_total,
                "sell_value_total":  sell_value_total,
                "queue_type":        queue_type
            }
        })

    return {
        "mode": "top_stocks",
        "date": date_str,
        "side": side,
        "scope": "top",
        "compare": compare,
        "count": len(items),
        "items": items
    }
