# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional, Dict, Any, DefaultDict
from collections import defaultdict

from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from backend.api.metadata import get_db  # AsyncSession provider
from datetime import date

router = APIRouter(prefix="", tags=["💧 Liquidity (Weekly)"])

@router.get("/liquidity/weekly/pivot", summary="ECharts-ready weekly liquidity (USD)")
async def liquidity_weekly_pivot(
    mode: str = Query("sector", description="sector | symbol"),
    # تاریخ‌ها
    date_to: date = Query(default_factory=date.today, description="YYYY-MM-DD (include weeks with week_end <= date_to)"),
    date_from: Optional[date] = Query(None, description="YYYY-MM-DD (optional range start)"),
    # محدودسازی صنایع (sector mode)
    sectors: Optional[str] = Query(None, description="comma-separated industries to include"),
    # فیلتر نماد (symbol mode)
    sector: Optional[str] = Query(None, description="(symbol mode) sector name"),
    symbol: Optional[str] = Query(None, description="(symbol mode) stock_ticker"),
    # تعداد هفته‌های اخیر روی محور X
    limit_weeks: Optional[int] = Query(12, description="limit number of recent weeks on X-axis"),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی مستقیم برای ECharts (bar-negative2):
    {
      "categories": ["2025-08-01", "2025-08-08", ...],   # week_end
      "series": [
        {"name":"بانکی","type":"bar","stack":"flow","emphasis":{"focus":"series"},"data":[...]}  # sector mode
        # یا
        {"name":"وبملت","type":"bar","stack":"flow","emphasis":{"focus":"series"},"data":[...]} # symbol mode
      ]
    }
    """
    try:
        if mode not in ("sector", "symbol"):
            raise HTTPException(status_code=400, detail="Invalid mode. Use 'sector' or 'symbol'.")

        # فیلترهای تاریخ
        filters = ['"week_end" <= :date_to']
        params: Dict[str, Any] = {"date_to": date_to}

        if date_from:
            filters.append('"week_end" >= :date_from')
            params["date_from"] = date_from

        if mode == "sector":
            # (اختیاری) محدودسازی صنایع
            if sectors:
                sector_list = [s.strip() for s in sectors.split(",") if s.strip()]
                filters.append('"sector" = ANY(:sectors)')
                params["sectors"] = sector_list

            where_clause = " AND ".join(filters) if filters else "TRUE"

            sql = f"""
                SELECT
                    COALESCE("sector", 'نامشخص') AS sector,
                    "week_end"::date AS week_end,
                    SUM(COALESCE("value_usd", 0)) AS total_value_usd
                FROM weekly_joined_data
                WHERE {where_clause}
                GROUP BY sector, week_end
                ORDER BY week_end ASC
            """
            result = await db.execute(text(sql), params)
            rows = [dict(r) for r in result.mappings().all()]

            # محور X: هفته‌ها
            unique_weeks = sorted({r["week_end"] for r in rows})
            if limit_weeks:
                unique_weeks = unique_weeks[-int(limit_weeks):]
            week_strs = [str(w) for w in unique_weeks]
            week_set = set(week_strs)

            # pivot: sector → week → sum
            sec_week_sum: DefaultDict[str, Dict[str, float]] = defaultdict(dict)
            all_sectors = set()
            for r in rows:
                wk = str(r["week_end"])
                if wk in week_set:
                    sec = r["sector"]
                    all_sectors.add(sec)
                    sec_week_sum[sec][wk] = float(r["total_value_usd"] or 0.0)

            series = []
            for sec in sorted(all_sectors):
                data_vec = [sec_week_sum[sec].get(wk, 0.0) for wk in week_strs]
                series.append({
                    "name": sec,
                    "type": "bar",
                    "stack": "flow",
                    "emphasis": {"focus": "series"},
                    "data": data_vec
                })

            return {
                "categories": week_strs,
                "series": series
            }

        # --- mode == "symbol" ---
        if not sector or not symbol:
            raise HTTPException(status_code=422, detail="In symbol mode, both 'sector' and 'symbol' are required.")

        filters += ['"sector" = :sector', '"stock_ticker" = :symbol']
        params.update({"sector": sector, "symbol": symbol})
        where_clause = " AND ".join(filters)

        sql = f"""
            SELECT
                "week_end"::date AS week_end,
                SUM(COALESCE("value_usd", 0)) AS total_value_usd
            FROM weekly_joined_data
            WHERE {where_clause}
            GROUP BY week_end
            ORDER BY week_end ASC
        """
        result = await db.execute(text(sql), params)
        rows = [dict(r) for r in result.mappings().all()]

        unique_weeks = sorted({r["week_end"] for r in rows})
        if limit_weeks:
            unique_weeks = unique_weeks[-int(limit_weeks):]
        week_strs = [str(w) for w in unique_weeks]
        week_set = set(week_strs)

        wk_sum = {str(r["week_end"]): float(r["total_value_usd"] or 0.0) for r in rows if str(r["week_end"]) in week_set}
        data_vec = [wk_sum.get(wk, 0.0) for wk in week_strs]

        series = [{
            "name": symbol,
            "type": "bar",
            "stack": "flow",
            "emphasis": {"focus": "series"},
            "data": data_vec
        }]

        return {
            "categories": week_strs,
            "series": series
        }

    except HTTPException:
        raise
    except Exception as e:
        # عمداً ساده: بدون wrapper
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")
