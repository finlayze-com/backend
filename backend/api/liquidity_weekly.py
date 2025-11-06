# backend/api/liquidity_weekly.py
# -*- coding: utf-8 -*-

from datetime import date
from typing import Optional, Dict, List, Tuple
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions

router = APIRouter(prefix="/liquidity/weekly", tags=["📈 Weekly Liquidity"])

# --- راهنمای ستون‌ها در weekly_joined_data ---
# week_end (date/timestamp), sector (text), stock_ticker (text)
# value (BIGINT / NUMERIC)                -> ارزش معاملات ریالی (ریال)
# value_usd (NUMERIC)                     -> ارزش معاملات دلاری
# buy_i_value, sell_i_value               -> خرید/فروش حقیقی (ریال)
# buy_i_value_usd, sell_i_value_usd       -> خرید/فروش حقیقی (دلاری)

# تقسیم بر 1e10 برای نمایش "میلیارد تومان"
RIAL_TO_TOMAN_BILLION_DIV = 1e10

def _metric_sql(metric: str) -> Tuple[str, str]:
    """
    برمی‌گرداند: (عبارتِ SQL برای جمع در سطح گروه‌بندی, توضیحِ واحد)
    """
    m = (metric or "").lower().strip()
    if m == "value":
        # ارزش معاملات ریالی → میلیارد تومان
        return f"SUM(value) / {RIAL_TO_TOMAN_BILLION_DIV}", "میلیارد تومان"
    elif m == "value_usd":
        # ارزش معاملات دلاری
        return "SUM(value_usd)", "USD"
    elif m == "net_flow":
        # ورود نقدینگی ریالی → میلیارد تومان
        return f"SUM(buy_i_value - sell_i_value) / {RIAL_TO_TOMAN_BILLION_DIV}", "میلیارد تومان"
    elif m == "net_flow_usd":
        # ورود نقدینگی دلاری
        return "SUM(buy_i_value_usd - sell_i_value_usd)", "USD"
    else:
        raise HTTPException(status_code=400, detail="Invalid metric. Use: value | value_usd | net_flow | net_flow_usd")

@router.get("/pivot", summary="Pivot هفتگی نقدینگی (sector | symbol | total)")
async def get_weekly_liquidity_pivot(
    mode: str = Query("sector", description="sector | symbol | total"),
    metric: str = Query("value_usd", description="value | value_usd | net_flow | net_flow_usd"),
    date_to: date = Query(default=date.today(), description="آخرین تاریخ شامل‌شونده"),
    date_from: Optional[date] = Query(default=None, description="اولین تاریخ شامل‌شونده"),
    # پارامتر مشترک: در mode=symbol اجباری، در mode=sector اختیاری (برای drill-down)
    sector: Optional[str] = Query(default=None, description="نام صنعت (در mode=symbol اجباری، در mode=sector اختیاری برای pivot نمادها)"),
    symbol: Optional[str] = Query(default=None, description="نماد (فقط در mode=symbol)"),
    limit_weeks: int = Query(12, ge=1, le=104, description="تعداد هفته‌های اخیر روی محور X"),
    # سورتِ لیستِ totals (در mode=total روی صنایع، و در mode=sector+sector روی نمادها)
    sort_by: str = Query(
        "value_desc",
        description="سورت totals: value_desc | value_asc | name_asc | name_desc"
    ),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Liquidity.WeeklyPivot","ALL"))
):
    """
    منبع داده: weekly_joined_data
    ستون‌ها: week_end, sector, stock_ticker, value, value_usd, buy_i_value, sell_i_value, buy_i_value_usd, sell_i_value_usd

    - mode=sector:
        الف) بدون پارامتر sector → pivot بر اساس صنایع (series=sectors)
        ب) با پارامتر sector → pivot بر اساس نمادهای همان صنعت (series=symbols) + total_timeseries + symbol_totals

    - mode=symbol:
        سری زمانی یک نماد مشخص در یک صنعت مشخص

    - mode=total:
        سری صنایع + سری Total (سری زمانی کل بازار) + sector_totals (جمع بازه برای هر صنعت) + total_timeseries
    """
    try:
        mode = (mode or "").lower().strip()
        if mode not in {"sector", "symbol", "total"}:
            raise HTTPException(status_code=400, detail="Invalid mode. Use 'sector', 'symbol' or 'total'.")

        metric_expr, unit_label = _metric_sql(metric)

        # -------------------- mode = symbol --------------------
        if mode == "symbol":
            if not sector or not symbol:
                raise HTTPException(status_code=422, detail="In symbol mode, both 'sector' and 'symbol' are required.")

            filters = ["week_end <= :date_to", "sector = :sector", "stock_ticker = :symbol"]
            params: Dict[str, object] = {"date_to": date_to, "sector": sector, "symbol": symbol}
            if date_from:
                filters.append("week_end >= :date_from")
                params["date_from"] = date_from

            q = text(f"""
                SELECT
                    week_end::date AS week_end,
                    {metric_expr} AS total_val
                FROM weekly_joined_data
                WHERE {" AND ".join(filters)}
                GROUP BY week_end
                ORDER BY week_end
            """)
            rows = (await db.execute(q, params)).mappings().all()

            categories = [r["week_end"].isoformat() for r in rows][-limit_weeks:]
            data = [float(r["total_val"] or 0.0) for r in rows][-limit_weeks:]

            return {
                "unit": unit_label,
                "metric": metric,
                "categories": categories,
                "series": [
                    {
                        "name": symbol,
                        "type": "bar",
                        "stack": "flow",
                        "emphasis": {"focus": "series"},
                        "data": data
                    }
                ]
            }

        # -------------------- mode = sector / total --------------------
        # فیلتر تاریخ پایه
        filters = ["week_end <= :date_to"]
        params: Dict[str, object] = {"date_to": date_to}
        if date_from:
            filters.append("week_end >= :date_from")
            params["date_from"] = date_from

        # حالت A: mode=sector و پارامتر sector «ست نشده» → pivot بر اساس صنایع
        if mode == "sector" and not sector:
            q = text(f"""
                SELECT
                    week_end::date AS week_end,
                    COALESCE(sector,'نامشخص') AS grp,
                    {metric_expr} AS total_val
                FROM weekly_joined_data
                WHERE {" AND ".join(filters)}
                GROUP BY grp, week_end
                ORDER BY week_end
            """)
            rows = (await db.execute(q, params)).mappings().all()

            all_weeks = sorted({r["week_end"].isoformat() for r in rows})
            if limit_weeks and len(all_weeks) > limit_weeks:
                all_weeks = all_weeks[-limit_weeks:]

            groups = sorted({r["grp"] for r in rows})
            series_map: Dict[str, Dict[str, float]] = {g: {} for g in groups}
            for r in rows:
                w = r["week_end"].isoformat()
                if w in all_weeks:
                    series_map[r["grp"]][w] = float(r["total_val"] or 0.0)

            series_list: List[Dict] = []
            for g in groups:
                data = [series_map[g].get(w, 0.0) for w in all_weeks]
                series_list.append({
                    "name": g,
                    "type": "bar",
                    "stack": "flow",
                    "emphasis": {"focus": "series"},
                    "data": data
                })

            return {
                "unit": unit_label,
                "metric": metric,
                "categories": all_weeks,
                "series": series_list
            }

        # حالت B: mode=sector و پارامتر sector «ست شده» → pivot بر اساس نمادهای همان صنعت
        if mode == "sector" and sector:
            filters_sym = filters + ["sector = :sector"]
            params_sym = dict(params)
            params_sym["sector"] = sector

            q = text(f"""
                SELECT
                    week_end::date AS week_end,
                    stock_ticker     AS grp,   -- گروه این حالت: نمادها
                    {metric_expr}    AS total_val
                FROM weekly_joined_data
                WHERE {" AND ".join(filters_sym)}
                GROUP BY grp, week_end
                ORDER BY week_end
            """)
            rows = (await db.execute(q, params_sym)).mappings().all()

            # محور X
            all_weeks = sorted({r["week_end"].isoformat() for r in rows})
            if limit_weeks and len(all_weeks) > limit_weeks:
                all_weeks = all_weeks[-limit_weeks:]

            symbols = sorted({r["grp"] for r in rows})
            series_map: Dict[str, Dict[str, float]] = {sym: {} for sym in symbols}
            for r in rows:
                w = r["week_end"].isoformat()
                if w in all_weeks:
                    series_map[r["grp"]][w] = float(r["total_val"] or 0.0)

            # سری‌های نمادها
            series_list: List[Dict] = []
            for sym in symbols:
                data = [series_map[sym].get(w, 0.0) for w in all_weeks]
                series_list.append({
                    "name": sym,
                    "type": "bar",
                    "stack": "flow",
                    "emphasis": {"focus": "series"},
                    "data": data
                })

            # جمع افقی (Total) برای همان صنعت
            total_data = []
            for w in all_weeks:
                total_w = 0.0
                for sym in symbols:
                    total_w += series_map[sym].get(w, 0.0)
                total_data.append(total_w)

            # Totals روی نمادها برای Pie/Bar رتبه‌ای
            symbol_totals = []
            for sym in symbols:
                s_sum = sum(series_map[sym].get(w, 0.0) for w in all_weeks)
                symbol_totals.append({"name": sym, "value": s_sum})

            # سورت
            sort_by_norm = (sort_by or "value_desc").lower().strip()
            if sort_by_norm not in {"value_desc", "value_asc", "name_asc", "name_desc"}:
                raise HTTPException(status_code=400, detail="Invalid sort_by. Use value_desc | value_asc | name_asc | name_desc")

            if sort_by_norm == "value_desc":
                symbol_totals.sort(key=lambda x: x["value"], reverse=True)
            elif sort_by_norm == "value_asc":
                symbol_totals.sort(key=lambda x: x["value"])
            elif sort_by_norm == "name_asc":
                symbol_totals.sort(key=lambda x: x["name"])
            elif sort_by_norm == "name_desc":
                symbol_totals.sort(key=lambda x: x["name"], reverse=True)

            return {
                "unit": unit_label,
                "metric": metric,
                "sector": sector,
                "categories": all_weeks,
                "series": series_list,                 # سری زمانی هر symbol
                "total_timeseries": {                  # جمع کل آن صنعت روی هر تاریخ
                    "name": "Total",
                    "data": total_data
                },
                "symbol_totals": symbol_totals         # یک عدد جمع برای هر symbol (در بازه)
            }

        # -------------------- mode = total --------------------
        # فیلتر تاریخ پایه
        q = text(f"""
            SELECT
                week_end::date AS week_end,
                COALESCE(sector,'نامشخص') AS grp,
                {metric_expr} AS total_val
            FROM weekly_joined_data
            WHERE {" AND ".join(filters)}
            GROUP BY grp, week_end
            ORDER BY week_end
        """)
        rows = (await db.execute(q, params)).mappings().all()

        all_weeks = sorted({r["week_end"].isoformat() for r in rows})
        if limit_weeks and len(all_weeks) > limit_weeks:
            all_weeks = all_weeks[-limit_weeks:]

        groups = sorted({r["grp"] for r in rows})
        series_map: Dict[str, Dict[str, float]] = {g: {} for g in groups}
        for r in rows:
            w = r["week_end"].isoformat()
            if w in all_weeks:
                series_map[r["grp"]][w] = float(r["total_val"] or 0.0)

        series_list: List[Dict] = []
        for g in groups:
            data = [series_map[g].get(w, 0.0) for w in all_weeks]
            series_list.append({
                "name": g,
                "type": "bar",
                "stack": "flow",
                "emphasis": {"focus": "series"},
                "data": data
            })

        # Total timeseries کل بازار
        total_data = []
        for w in all_weeks:
            total_w = 0.0
            for g in groups:
                total_w += series_map[g].get(w, 0.0)
            total_data.append(total_w)

        # Totals روی صنایع
        sector_totals = []
        for g in groups:
            g_sum = sum(series_map[g].get(w, 0.0) for w in all_weeks)
            sector_totals.append({"name": g, "value": g_sum})

        sort_by_norm = (sort_by or "value_desc").lower().strip()
        if sort_by_norm not in {"value_desc", "value_asc", "name_asc", "name_desc"}:
            raise HTTPException(status_code=400, detail="Invalid sort_by. Use value_desc | value_asc | name_asc | name_desc")

        if sort_by_norm == "value_desc":
            sector_totals.sort(key=lambda x: x["value"], reverse=True)
        elif sort_by_norm == "value_asc":
            sector_totals.sort(key=lambda x: x["value"])
        elif sort_by_norm == "name_asc":
            sector_totals.sort(key=lambda x: x["name"])
        elif sort_by_norm == "name_desc":
            sector_totals.sort(key=lambda x: x["name"], reverse=True)

        series_list.append({
            "name": "Total",
            "type": "bar",
            "stack": "flow",
            "emphasis": {"focus": "series"},
            "data": total_data
        })

        return {
            "unit": unit_label,
            "metric": metric,
            "categories": all_weeks,
            "series": series_list,             # سری‌های صنایع + Total (سری زمانی)
            "sector_totals": sector_totals,    # جمع بازه برای هر صنعت
            "total_timeseries": {              # سری زمانی کل بازار
                "name": "Total",
                "data": total_data
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")
