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

# تقسیم بر 1e10 برای نمایش "میلیارد تومان"
RIAL_TO_TOMAN_BILLION_DIV = 1e10


def _metric_sql(metric: str) -> Tuple[str, str]:
    """
    برمی‌گرداند: (عبارت SQL برای جمع در سطح گروه‌بندی, توضیح واحد)
    """
    m = (metric or "").lower().strip()
    if m == "value":
        return f"SUM(value) / {RIAL_TO_TOMAN_BILLION_DIV}", "میلیارد تومان"
    elif m == "value_usd":
        return "SUM(value_usd)", "USD"
    elif m == "net_flow":
        return f"SUM(buy_i_value - sell_i_value) / {RIAL_TO_TOMAN_BILLION_DIV}", "میلیارد تومان"
    elif m == "net_flow_usd":
        return "SUM(buy_i_value_usd - sell_i_value_usd)", "USD"
    else:
        raise HTTPException(status_code=400, detail="Invalid metric. Use: value | value_usd | net_flow | net_flow_usd")


async def _compute_window(
    db: AsyncSession,
    base_filters: List[str],
    params: Dict[str, object],
    limit_weeks: int,
) -> Tuple[Optional[date], Optional[date], List[str]]:
    """
    هفته‌های مؤثر، با قوانین:
      - اگر date_from ست باشد: کل بازهٔ date_from..date_to (limit_weeks بی‌اثر)
      - اگر date_from ست نباشد: آخرین limit_weeks هفته تا date_to
    """
    q = text(f"""
        SELECT DISTINCT week_end::date AS w
        FROM weekly_joined_data
        WHERE {" AND ".join(base_filters)}
        ORDER BY w
    """)
    rows = (await db.execute(q, params)).mappings().all()
    weeks_all = [r["w"].isoformat() for r in rows]

    if not weeks_all:
        return None, None, []

    if "date_from" in params and params["date_from"]:
        # بازه دقیقاً از date_from تا date_to
        weeks_eff = weeks_all
    else:
        # بازه = آخرین limit_weeks هفته
        if limit_weeks > 0 and len(weeks_all) > limit_weeks:
            weeks_eff = weeks_all[-limit_weeks:]
        else:
            weeks_eff = weeks_all

    wmin = date.fromisoformat(weeks_eff[0])
    wmax = date.fromisoformat(weeks_eff[-1])
    return wmin, wmax, weeks_eff


async def _pie_value_usd_by_sector_range(
    db: AsyncSession,
    wmin: Optional[date],
    wmax: Optional[date]
) -> Dict:
    """
    Pie صنایع روی بازهٔ مؤثر (همیشه با value_usd، مستقل از metric انتخابی)
    """
    if not wmin or not wmax:
        return {"week_end": None, "unit": "USD", "items": []}

    q = text("""
        SELECT
            COALESCE(sector, 'نامشخص') AS sector_name,
            SUM(value_usd)              AS total_value_usd
        FROM weekly_joined_data
        WHERE week_end BETWEEN :wmin AND :wmax
        GROUP BY sector_name
        ORDER BY total_value_usd DESC NULLS LAST
    """)
    rows = (await db.execute(q, {"wmin": wmin, "wmax": wmax})).mappings().all()
    items = [{"name": r["sector_name"], "value": float(r["total_value_usd"] or 0.0)} for r in rows]
    return {"week_end": wmax.isoformat(), "unit": "USD", "items": items}


async def _pie_value_usd_by_symbols_of_sector_range(
    db: AsyncSession,
    wmin: Optional[date],
    wmax: Optional[date],
    sector: Optional[str]
) -> Dict:
    """
    Pie نمادهای یک صنعت روی بازهٔ مؤثر (همیشه با value_usd، مستقل از metric انتخابی)
    """
    if not wmin or not wmax or not sector:
        return {"week_end": None, "unit": "USD", "sector": sector, "items": []}

    q = text("""
        SELECT
            stock_ticker    AS symbol_name,
            SUM(value_usd)  AS total_value_usd
        FROM weekly_joined_data
        WHERE week_end BETWEEN :wmin AND :wmax
          AND sector = :sector
        GROUP BY symbol_name
        ORDER BY total_value_usd DESC NULLS LAST
    """)
    rows = (await db.execute(q, {"wmin": wmin, "wmax": wmax, "sector": sector})).mappings().all()
    items = [{"name": r["symbol_name"], "value": float(r["total_value_usd"] or 0.0)} for r in rows]
    return {"week_end": wmax.isoformat(), "unit": "USD", "sector": sector, "items": items}


@router.get("/pivot", summary="Pivot هفتگی نقدینگی (sector | total) با خروجی یکپارچه")
async def get_weekly_liquidity_pivot(
    mode: str = Query("sector", description="sector | total"),
    metric: str = Query("value_usd", description="value | value_usd | net_flow | net_flow_usd"),
    date_to: date = Query(default=date.today(), description="آخرین تاریخ شامل‌شونده"),
    date_from: Optional[date] = Query(default=None, description="اولین تاریخ شامل‌شونده"),
    sector: Optional[str] = Query(default=None, description="نام صنعت"),
    symbol: Optional[str] = Query(default=None, description="نماد (در صورت ست بودن، sector اجباری است)"),
    limit_weeks: int = Query(12, ge=0, description="تعداد هفته‌های اخیر (0 = بدون محدودیت)"),
    sort_by: str = Query("value_desc", description="برای sector_totals در حالت total: value_desc | value_asc | name_asc | name_desc"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Report.Liquidity.WeeklyPivot","ALL"))
):
    """
    منبع داده: weekly_joined_data
    خروجی همیشه فقط شامل این 3 کلید است:
      - sector_totals
      - total_timeseries = { name, unit, weeks, data }
      - fix_value_pie
    و هر سه دقیقاً در یک «بازهٔ مؤثر» محاسبه می‌شوند:
      - اگر date_from ست باشد: date_from..date_to
      - اگر date_from ست نباشد: آخرین limit_weeks هفته تا date_to
    """
    try:
        mode = (mode or "").lower().strip()
        if mode not in {"sector", "total"}:
            raise HTTPException(status_code=400, detail="Invalid mode. Use 'sector' or 'total'.")

        metric_expr, unit_label = _metric_sql(metric)

        # ===== فیلتر تاریخ پایه =====
        base_filters = ["week_end <= :date_to"]
        params: Dict[str, object] = {"date_to": date_to}
        if date_from:
            base_filters.append("week_end >= :date_from")
            params["date_from"] = date_from

        # ===== محاسبه پنجره مؤثر (wmin,wmax,weeks) یکبار و استفاده همه‌جا =====
        wmin, wmax, weeks = await _compute_window(db, base_filters, params, limit_weeks)
        if not weeks:
            # خروجی خالی
            return {
                "sector_totals": [],
                "total_timeseries": {"name": "Total", "unit": unit_label, "weeks": [], "data": []},
                "fix_value_pie": {"week_end": None, "unit": "USD", "items": []}
            }

        # ===================== حالت: TOTAL =====================
        if mode == "total":
            # total_timeseries: جمع کل بازار در هر هفته (در بازه مؤثر)
            q_ts = text(f"""
                SELECT week_end::date AS week_end,
                       {metric_expr}  AS total_val
                FROM weekly_joined_data
                WHERE week_end BETWEEN :wmin AND :wmax
                GROUP BY week_end
                ORDER BY week_end
            """)
            rows_ts = (await db.execute(q_ts, {"wmin": wmin, "wmax": wmax})).mappings().all()
            data_map = {r["week_end"].isoformat(): float(r["total_val"] or 0.0) for r in rows_ts}
            data = [data_map.get(w, 0.0) for w in weeks]

            # sector_totals: جمع بازه برای صنایع (در بازه مؤثر)
            q_tot = text(f"""
                SELECT COALESCE(sector,'نامشخص') AS grp,
                       SUM(inner_val)             AS gsum
                FROM (
                    SELECT sector, week_end, {metric_expr} AS inner_val
                    FROM weekly_joined_data
                    WHERE week_end BETWEEN :wmin AND :wmax
                    GROUP BY sector, week_end
                ) t
                GROUP BY grp
            """)
            rows_tot = (await db.execute(q_tot, {"wmin": wmin, "wmax": wmax})).mappings().all()
            sector_totals = [{"name": r["grp"], "value": float(r["gsum"] or 0.0)} for r in rows_tot]

            # مرتب‌سازی
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

            # Pie صنایع (value_usd) در همان بازه مؤثر
            fix_value_pie = await _pie_value_usd_by_sector_range(db, wmin, wmax)

            return {
                "sector_totals": sector_totals,
                "total_timeseries": {"name": "Total", "unit": unit_label, "weeks": weeks, "data": data},
                "fix_value_pie": fix_value_pie
            }

        # ===================== حالت: SECTOR =====================
        # اگر sector انتخاب نشده: مثل total
        if not sector:
            # همان منطق total استفاده می‌شود
            q_ts = text(f"""
                SELECT week_end::date AS week_end,
                       {metric_expr}  AS total_val
                FROM weekly_joined_data
                WHERE week_end BETWEEN :wmin AND :wmax
                GROUP BY week_end
                ORDER BY week_end
            """)
            rows_ts = (await db.execute(q_ts, {"wmin": wmin, "wmax": wmax})).mappings().all()
            data_map = {r["week_end"].isoformat(): float(r["total_val"] or 0.0) for r in rows_ts}
            data = [data_map.get(w, 0.0) for w in weeks]

            q_tot = text(f"""
                SELECT COALESCE(sector,'نامشخص') AS grp,
                       SUM(inner_val)             AS gsum
                FROM (
                    SELECT sector, week_end, {metric_expr} AS inner_val
                    FROM weekly_joined_data
                    WHERE week_end BETWEEN :wmin AND :wmax
                    GROUP BY sector, week_end
                ) t
                GROUP BY grp
            """)
            rows_tot = (await db.execute(q_tot, {"wmin": wmin, "wmax": wmax})).mappings().all()
            sector_totals = [{"name": r["grp"], "value": float(r["gsum"] or 0.0)} for r in rows_tot]

            fix_value_pie = await _pie_value_usd_by_sector_range(db, wmin, wmax)

            return {
                "sector_totals": sector_totals,
                "total_timeseries": {"name": "Total", "unit": unit_label, "weeks": weeks, "data": data},
                "fix_value_pie": fix_value_pie
            }

        # اگر symbol ست شده باشد:
        if symbol:
            # سری زمانی نماد انتخابی در بازه مؤثر
            q_ts = text(f"""
                SELECT week_end::date AS week_end,
                       {metric_expr}  AS total_val
                FROM weekly_joined_data
                WHERE week_end BETWEEN :wmin AND :wmax
                  AND sector = :sector AND stock_ticker = :symbol
                GROUP BY week_end
                ORDER BY week_end
            """)
            rows_ts = (await db.execute(q_ts, {"wmin": wmin, "wmax": wmax, "sector": sector, "symbol": symbol})).mappings().all()
            data_map = {r["week_end"].isoformat(): float(r["total_val"] or 0.0) for r in rows_ts}
            data = [data_map.get(w, 0.0) for w in weeks]

            # جمع بازه برای نمادهای همین صنعت
            q_tot = text(f"""
                SELECT stock_ticker AS sym, SUM(inner_val) AS gsum
                FROM (
                    SELECT stock_ticker, week_end, {metric_expr} AS inner_val
                    FROM weekly_joined_data
                    WHERE week_end BETWEEN :wmin AND :wmax
                      AND sector = :sector
                    GROUP BY stock_ticker, week_end
                ) t
                GROUP BY sym
                ORDER BY gsum DESC NULLS LAST
            """)
            rows_tot = (await db.execute(q_tot, {"wmin": wmin, "wmax": wmax, "sector": sector})).mappings().all()
            sector_totals = [{"name": r["sym"], "value": float(r["gsum"] or 0.0)} for r in rows_tot]

            # Pie نمادهای صنعت (value_usd) در بازه مؤثر
            fix_value_pie = await _pie_value_usd_by_symbols_of_sector_range(db, wmin, wmax, sector)

            return {
                "sector_totals": sector_totals,
                "total_timeseries": {"name": symbol, "unit": unit_label, "weeks": weeks, "data": data},
                "fix_value_pie": fix_value_pie
            }

        # فقط sector ست شده (symbol خالی):
        # سری زمانی جمع همان صنعت
        q_ts = text(f"""
            SELECT week_end::date AS week_end,
                   {metric_expr}  AS total_val
            FROM weekly_joined_data
            WHERE week_end BETWEEN :wmin AND :wmax
              AND sector = :sector
            GROUP BY week_end
            ORDER BY week_end
        """)
        rows_ts = (await db.execute(q_ts, {"wmin": wmin, "wmax": wmax, "sector": sector})).mappings().all()
        data_map = {r["week_end"].isoformat(): float(r["total_val"] or 0.0) for r in rows_ts}
        data = [data_map.get(w, 0.0) for w in weeks]

        # جمع بازه برای نمادهای همین صنعت
        q_tot = text(f"""
            SELECT stock_ticker AS sym, SUM(inner_val) AS gsum
            FROM (
                SELECT stock_ticker, week_end, {metric_expr} AS inner_val
                FROM weekly_joined_data
                WHERE week_end BETWEEN :wmin AND :wmax
                  AND sector = :sector
                GROUP BY stock_ticker, week_end
            ) t
            GROUP BY sym
            ORDER BY gsum DESC NULLS LAST
        """)
        rows_tot = (await db.execute(q_tot, {"wmin": wmin, "wmax": wmax, "sector": sector})).mappings().all()
        sector_totals = [{"name": r["sym"], "value": float(r["gsum"] or 0.0)} for r in rows_tot]

        # Pie نمادهای همان صنعت (value_usd) در بازه مؤثر
        fix_value_pie = await _pie_value_usd_by_symbols_of_sector_range(db, wmin, wmax, sector)

        return {
            "sector_totals": sector_totals,
            "total_timeseries": {"name": sector, "unit": unit_label, "weeks": weeks, "data": data},
            "fix_value_pie": fix_value_pie
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")
