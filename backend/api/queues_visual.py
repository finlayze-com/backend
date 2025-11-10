# backend/api/queues_visual.py
# -*- coding: utf-8 -*-
"""
API نمایش صف‌ها برای فرانت:
- GET /queues/treemap  : ترِی‌مپِ صف‌ها (گروه‌بندی بر اساس صنعت) با رنگ امضادار (خرید-فروش)
- GET /queues/bullet   : بولت‌چارت مقایسه «ارزش صف» با base_value و/یا day_value

پیش‌نیاز جداول:
- quote(
    inscode text, stock_ticker text, date text(YYYY-MM-DD),
    BQ_Value bigint, SQ_Value bigint, Value bigint, base_value bigint, ...
  )
- symboldetail("insCode" text, sector text, stock_ticker text, instrument_type text)

نکته:
- industry/sector از جدول symboldetail خوانده می‌شود (جوین روی insCode←→inscode).
"""

from typing import Optional, Literal, Dict, Any, List, Tuple
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions

router = APIRouter(prefix="/queues", tags=["📊 Queues Visuals"])

# --------------------------- Helpers ---------------------------

async def _latest_quote_date(db: AsyncSession) -> str:
    """
    آخرین تاریخ موجود در quote (فرمت YYYY-MM-DD)
    """
    q = text("""SELECT MAX(q."date") AS d FROM quote q""")
    r = await db.execute(q)
    d = r.scalar()
    if not d:
        raise HTTPException(status_code=404, detail="no date in quote")
    return d

def _queue_value_case(side: Literal["buy", "sell", "both"]) -> str:
    """
    عبارت SQL برای محاسبه ارزش صف بر اساس سمت صف
    - buy  → BQ_Value
    - sell → SQ_Value
    - both → BQ_Value + SQ_Value
    """
    if side == "buy":
        return 'COALESCE(q."BQ_Value", 0)'
    if side == "sell":
        return 'COALESCE(q."SQ_Value", 0)'
    return 'COALESCE(q."BQ_Value", 0) + COALESCE(q."SQ_Value", 0)'

def _presence_filter(side: Literal["buy", "sell", "both"]) -> str:
    """
    فقط نمادهای «صف‌دار» را نگه‌دار (صفرها حذف شوند)
    """
    if side == "buy":
        return 'AND COALESCE(q."BQ_Value", 0) > 0'
    if side == "sell":
        return 'AND COALESCE(q."SQ_Value", 0) > 0'
    # both: حداقل یکی > 0 باشد
    return 'AND (COALESCE(q."BQ_Value",0) > 0 OR COALESCE(q."SQ_Value",0) > 0)'

# --------------------------- Treemap ---------------------------

@router.get("/treemap", summary="Treemap of queues grouped by sector (ECharts-friendly)")
async def queues_treemap(
    date: Optional[str] = Query(None, description="YYYY-MM-DD؛ اگر خالی باشد آخرین تاریخ quote"),
    side: Literal["buy", "sell", "both"] = Query(
        "buy", description="سمت صف برای سایز جعبه‌ها: buy/sell/both"
    ),
    metric: Literal["queue", "base", "value"] = Query(
        "queue",
        description="اندازهٔ جعبه‌ها: queue=ارزش صف، base=base_value، value=ارزش معاملات روز"
    ),
    sector: Optional[str] = Query(None, description="اگر مقدار بدهید فقط همان صنعت برگردانده می‌شود"),
    min_value: Optional[int] = Query(None, description="فیلتر: فقط رکوردهای با مقدار ≥ این عدد"),
    _ = Depends(require_permissions("Report.Queues.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی برای Treemap به‌صورت ساختار ECharts:

    {
      "date": "...",
      "side": "buy|sell|both",
      "metric": "queue|base|value",
      "color_scale": {"min": -X, "max": +Y},
      "children": [
        {
          "name": "صنعت X",
          "value": <جمع اندازه در آن صنعت>,
          "color_value": <جمع خالص رنگ در سطح صنعت>,
          "children": [
              {"name": "نماد1", "value": ..., "color_value": net(BQ-SQ)},
              {"name": "نماد2", "value": ..., "color_value": ...},
              ...
          ]
        }, ...
      ]
    }

    توضیح رنگ: color_value = (BQ_Value - SQ_Value)  ⇒ مثبت = غلبه خرید، منفی = غلبه فروش
    """
    if date is None:
        date = await _latest_quote_date(db)

    qexpr = _queue_value_case(side)
    queue_presence_filter = _presence_filter(side)

    # اندازهٔ جعبه‌ها
    if metric == "queue":
        size_expr = qexpr
    elif metric == "base":
        size_expr = 'COALESCE(q."base_value", 0)'
    else:
        size_expr = 'COALESCE(q."Value", 0)'

    # مقدار رنگ (امضادار)
    color_expr = '(COALESCE(q."BQ_Value",0) - COALESCE(q."SQ_Value",0))'

    # فیلتر صنعت (اختیاری)
    sector_filter_sql = ""
    params: Dict[str, Any] = {"date": date}
    if sector:
        sector_filter_sql = 'AND sd."sector" = :sector'
        params["sector"] = sector

    # داده‌های برگ (symbol-level)
    leaf_sql = f"""
        SELECT
            sd."sector"        AS sector,
            q."stock_ticker"   AS stock_ticker,
            ({size_expr})      AS box_value,
            ({color_expr})     AS color_value
        FROM quote q
        JOIN symboldetail sd
          ON sd."insCode"::text = q."inscode"::text
        WHERE q."date" = :date
          AND sd."sector" IS NOT NULL
          {sector_filter_sql}
          {queue_presence_filter}
    """
    res = await db.execute(text(leaf_sql), params)
    rows = res.mappings().all()

    if not rows:
        return {"date": date, "side": side, "metric": metric, "children": [], "color_scale": {"min": 0, "max": 0}}

    # فیلتر حداقل مقدار (اختیاری) و ساخت leaves
    leaves: List[Dict[str, Any]] = []
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
        leaves.append({"sector": r["sector"], "name": r["stock_ticker"], "value": v, "color_value": c})

    if not leaves:
        return {"date": date, "side": side, "metric": metric, "children": [], "color_scale": {"min": 0, "max": 0}}

    # گروه‌بندی پایتونی برای ساختار Treemap
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

    # فقط صنایعی که حداقل یک بچه با value>0 دارند
    children = [v for v in sector_bucket.values() if v["value"] > 0]
    # مرتب‌سازی نزولی بر اساس ارزش کل صنعت
    children.sort(key=lambda x: x["value"], reverse=True)

    return {
        "date": date,
        "side": side,
        "metric": metric,
        "color_scale": {"min": int(color_min), "max": int(color_max)},
        "children": children
    }

# --------------------------- Bullet ---------------------------
# خروجی مینیمال: بدون ranges و داده‌های اضافی
# marker: فقط یکی از base یا value (گزینه both حذف شد)

@router.get("/bullet", summary="Bullet data: top stocks, sector stocks, or single stock (minimal)")
async def queues_bullet(
    date: Optional[str] = Query(None, description="YYYY-MM-DD؛ اگر خالی باشد آخرین تاریخ quote"),
    scope: Literal["top", "sector", "stock"] = Query("top", description="دامنه گزارش"),
    side: Literal["buy", "sell", "both"] = Query("buy", description="سمت صف"),
    marker: Literal["none", "base", "value"] = Query("none", description="مارکر: none|base|value"),
    top_n: int = Query(10, ge=1, le=100, description="تعداد آیتم‌ها در حالت top"),
    sector: Optional[str] = Query(None, description="نام صنعت (برای scope=sector الزامی)"),
    stock: Optional[str] = Query(None, description="نماد (برای scope=stock الزامی)"),
    _ = Depends(require_permissions("Report.Queues.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی مینیمال و واضح:

    اگر side = buy یا sell:
      {
        "mode": "...",
        "date": "...",
        "side": "buy|sell",
        "marker": "none|base|value",
        "count": N,
        "items": [
          { "title": "نماد", "measure": <ارزش صف سمت انتخابی>, "marker": <درصورت درخواست> },
          ...
        ]
      }

    اگر side = both:
      {
        "mode": "...",
        "date": "...",
        "side": "both",
        "marker": "none|base|value",
        "count": N,
        "items": [
          { "title": "نماد", "buy": <BQ_Value>, "sell": <SQ_Value>, "marker": <درصورت درخواست> },
          ...
        ]
      }
    """
    if date is None:
        date = await _latest_quote_date(db)

    # فیلد مارکر انتخابی
    marker_expr = _marker_field(marker)

    # پایه SELECT مشترک
    base_select = f"""
        SELECT
            q."stock_ticker"                           AS stock,
            SUM(COALESCE(q."BQ_Value",0))              AS buy,
            SUM(COALESCE(q."SQ_Value",0))              AS sell,
            {marker_expr}                              AS marker_val
        FROM quote q
    """

    where_and_group = """
        WHERE q."date" = :date
        GROUP BY q."stock_ticker"
        HAVING SUM(COALESCE(q."BQ_Value",0)) > 0 OR SUM(COALESCE(q."SQ_Value",0)) > 0
    """

    params: Dict[str, Any] = {"date": date}

    # ترتیب بر اساس سمت
    order_expr = _order_expr_for_side(side)

    # --- حالت‌های دامنه ---
    if scope == "top":
        sql = base_select + where_and_group + f"\nORDER BY {order_expr}\nLIMIT :top_n"
        params["top_n"] = top_n

    elif scope == "sector":
        if not sector:
            raise HTTPException(status_code=400, detail="sector is required for scope=sector")
        sql = base_select + """
            JOIN symboldetail sd
              ON sd."insCode"::text = q."inscode"::text
            WHERE q."date" = :date
              AND sd."sector" = :sector
            GROUP BY q."stock_ticker"
            HAVING SUM(COALESCE(q."BQ_Value",0)) > 0 OR SUM(COALESCE(q."SQ_Value",0)) > 0
        """ + f"\nORDER BY {order_expr}"
        params["sector"] = sector

    else:  # scope == "stock"
        if not stock:
            raise HTTPException(status_code=400, detail="stock is required for scope=stock")
        sql = base_select + """
            WHERE q."date" = :date
              AND q."stock_ticker" = :stock
            GROUP BY q."stock_ticker"
            HAVING SUM(COALESCE(q."BQ_Value",0)) > 0 OR SUM(COALESCE(q."SQ_Value",0)) > 0
        """
        params["stock"] = stock

    res = await db.execute(text(sql), params)
    rows = res.mappings().all()

    # ساخت خروجی مینیمال
    items: List[Dict[str, Any]] = []
    for r in rows:
        title = r["stock"]
        buy_v = int(r["buy"] or 0)
        sell_v = int(r["sell"] or 0)
        mark_v = (int(r["marker_val"]) if r["marker_val"] is not None else None)

        if side in ("buy", "sell"):
            measure = buy_v if side == "buy" else sell_v
            # فقط موارد واقعاً صف‌دارِ همان سمت را نگه داریم
            if measure <= 0:
                continue
            out: Dict[str, Any] = {"title": title, "measure": measure}
            if marker != "none":
                out["marker"] = mark_v or 0
            items.append(out)
        else:
            # both → خرید و فروش جداگانه
            if (buy_v <= 0 and sell_v <= 0):
                continue
            out = {"title": title, "buy": buy_v, "sell": sell_v}
            if marker != "none":
                out["marker"] = mark_v or 0
            items.append(out)

    mode = (
        "top_stocks" if scope == "top"
        else "sector_stocks" if scope == "sector"
        else "single_stock"
    )

    return {
        "mode": mode,
        "date": date,
        "side": side,
        "marker": marker,
        "count": len(items),
        "items": items
    }
