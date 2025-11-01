# backend/api/ceiling.py
# -*- coding: utf-8 -*-
from typing import Optional, Literal, List
import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/targets", tags=["🎯 Ceiling Targets"])

DAILY_SRC = "daily_joined_data"  # اگر ویو نداری، "daily_joined_data"

def _resolve_price_cols(adjusted: bool, currency: Literal["rial", "usd"]):
    base_close = "adjust_close" if adjusted else "close"
    base_final = "adjust_final_price" if adjusted else "final_price"
    if currency == "usd":
        base_close += "_usd"
        base_final += "_usd"
    return base_close, base_final


@router.get("/ceiling", summary="Gap-to-Ceiling (ATH یا بازه start/end)")
async def ceiling_targets(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date:   Optional[str] = Query(None, description="YYYY-MM-DD"),
    sector:     Optional[str] = Query(None, description="symboldetail.sector"),
    adjusted:   bool          = Query(True),
    currency:   Literal["rial", "usd"] = Query("rial"),
    _ = Depends(require_permissions("Report.Ceiling.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی: [{ stock_ticker, price_now, ceiling_price, gap_abs, gap_pct, hit }]
    قوانین تاریخ:
      - هر دو تاریخ ⇒ rolling_high روی [start_date, end_date]
      - فقط start_date ⇒ end_date = امروز (اگر امروز دیتا نبود، می‌شود آخرین تاریخ موجود)
      - فقط end_date ⇒ 400
      - هیچ‌کدام ⇒ ATH
    """
    try:
        price_col, final_col = _resolve_price_cols(adjusted, currency)

        # اعتبارسنجی: فقط end_date ممنوع
        if (end_date and not start_date):
            return create_response(
                status="error", status_code=400,
                message="وقتی end_date می‌دهید باید start_date هم بدهید.",
                data=[]
            )

        # حالت Rolling: اگر start_date هست (چه end_date باشد چه نباشد)
        if start_date:
            # end_anchor: اگر end_date ندادیم ⇒ امروز؛
            # و برای جلوگیری از خروجی خالی، به آخرین تاریخ موجود clamp می‌کنیم.
            sql = f"""
            WITH params AS (
              SELECT
                :start_date::date                       AS start_date,
                COALESCE(:end_date::date, CURRENT_DATE) AS end_wish
            ),
            end_anchor AS (
              SELECT LEAST(p.end_wish,
                           (SELECT max(date_miladi) FROM {DAILY_SRC})) AS end_date
              FROM params p
            ),
            base AS (
              SELECT dj.stock_ticker, dj.date_miladi, dj.{final_col} AS price_now
              FROM {DAILY_SRC} dj, end_anchor ea
              WHERE dj.date_miladi = ea.end_date
            ),
            wnd AS (
              SELECT dj.stock_ticker,
                     MAX(dj.{price_col}) AS ceiling_price
              FROM {DAILY_SRC} dj
              JOIN end_anchor ea ON TRUE
              WHERE dj.date_miladi BETWEEN :start_date AND ea.end_date
              GROUP BY dj.stock_ticker
            )
            SELECT
              b.stock_ticker,
              b.price_now,
              w.ceiling_price,
              (w.ceiling_price - b.price_now) AS gap_abs,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (w.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct,
              (b.price_now >= w.ceiling_price - 1e-9) AS hit
            FROM base b
            JOIN wnd  w USING (stock_ticker)
            { 'JOIN (SELECT stock_ticker FROM symboldetail WHERE sector = :sector) s USING (stock_ticker)' if sector else '' }
            ORDER BY gap_pct DESC NULLS LAST;
            """
            q = text(sql).bindparams(
                start_date=start_date,
                end_date=end_date,
                **({"sector": sector} if sector else {})
            )
            rows = (await db.execute(q)).mappings().all()
            return create_response(data=[dict(r) for r in rows])

        # حالت ATH: هیچ تاریخی نیامده
        sql = f"""
        WITH base AS (
          SELECT stock_ticker, date_miladi, {final_col} AS price_now
          FROM {DAILY_SRC}
          WHERE date_miladi = (SELECT max(date_miladi) FROM {DAILY_SRC})
        ),
        ath AS (
          SELECT stock_ticker, max({price_col}) AS ceiling_price
          FROM {DAILY_SRC}
          GROUP BY stock_ticker
        )
        SELECT
          b.stock_ticker,
          b.price_now,
          a.ceiling_price,
          (a.ceiling_price - b.price_now) AS gap_abs,
          CASE WHEN b.price_now > 0
               THEN 100.0 * (a.ceiling_price - b.price_now)/b.price_now
               ELSE NULL END AS gap_pct,
          (b.price_now >= a.ceiling_price - 1e-9) AS hit
        FROM base b
        JOIN ath a USING (stock_ticker)
        { 'JOIN (SELECT stock_ticker FROM symboldetail WHERE sector = :sector) s USING (stock_ticker)' if sector else '' }
        ORDER BY gap_pct DESC NULLS LAST;
        """
        q = text(sql).bindparams(**({"sector": sector} if sector else {}))
        rows = (await db.execute(q)).mappings().all()
        return create_response(data=[dict(r) for r in rows])

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("ceiling_targets failed")
        raise HTTPException(status_code=500, detail="Internal error in /targets/ceiling: " + str(e))


@router.get("/ceiling/funnel", summary="Funnel buckets of gap-to-ceiling (range-based)")
async def ceiling_funnel(
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    sector:     Optional[str] = Query(None),
    adjusted:   bool          = Query(True),
    currency:   Literal["rial", "usd"] = Query("rial"),
    bins:       List[float]   = Query([1, 2, 5, 10]),
    _ = Depends(require_permissions("Report.Ceiling.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی: [{ name: '≤1%', value: N }, { name: '1–2%', value: N }, ... , { name: '>last', value: N }]
    همان قواعد تاریخ:
      - هر دو تاریخ ⇒ بازه
      - فقط start_date ⇒ end = امروز (clamp به آخرین تاریخ موجود)
      - فقط end_date ⇒ 400
      - هیچ‌کدام ⇒ ATH
    """
    try:
        price_col, final_col = _resolve_price_cols(adjusted, currency)

        if (end_date and not start_date):
            return create_response(
                status="error", status_code=400,
                message="وقتی end_date می‌دهید باید start_date هم بدهید.",
                data=[]
            )

        if start_date:
            sql = f"""
            WITH params AS (
              SELECT
                :start_date::date                       AS start_date,
                COALESCE(:end_date::date, CURRENT_DATE) AS end_wish
            ),
            end_anchor AS (
              SELECT LEAST(p.end_wish,
                           (SELECT max(date_miladi) FROM {DAILY_SRC})) AS end_date
              FROM params p
            ),
            base AS (
              SELECT dj.stock_ticker, dj.date_miladi, dj.{final_col} AS price_now
              FROM {DAILY_SRC} dj, end_anchor ea
              WHERE dj.date_miladi = ea.end_date
            ),
            wnd AS (
              SELECT dj.stock_ticker,
                     MAX(dj.{price_col}) AS ceiling_price
              FROM {DAILY_SRC} dj
              JOIN end_anchor ea ON TRUE
              WHERE dj.date_miladi BETWEEN :start_date AND ea.end_date
              GROUP BY dj.stock_ticker
            )
            SELECT
              b.stock_ticker,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (w.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct
            FROM base b
            JOIN wnd  w USING (stock_ticker)
            { 'JOIN (SELECT stock_ticker FROM symboldetail WHERE sector = :sector) s USING (stock_ticker)' if sector else '' };
            """
            q = text(sql).bindparams(
                start_date=start_date,
                end_date=end_date,
                **({"sector": sector} if sector else {})
            )
        else:
            # ATH
            sql = f"""
            WITH base AS (
              SELECT stock_ticker, date_miladi, {final_col} AS price_now
              FROM {DAILY_SRC}
              WHERE date_miladi = (SELECT max(date_miladi) FROM {DAILY_SRC})
            ),
            ath AS (
              SELECT stock_ticker, max({price_col}) AS ceiling_price
              FROM {DAILY_SRC}
              GROUP BY stock_ticker
            )
            SELECT
              b.stock_ticker,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (a.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct
            FROM base b
            JOIN ath a USING (stock_ticker)
            { 'JOIN (SELECT stock_ticker FROM symboldetail WHERE sector = :sector) s USING (stock_ticker)' if sector else '' };
            """
            q = text(sql).bindparams(**({"sector": sector} if sector else {}))

        rows = (await db.execute(q)).mappings().all()
        gaps = [r["gap_pct"] for r in rows if r["gap_pct"] is not None]

        thresholds = sorted(bins)
        buckets = [0] * (len(thresholds) + 1)
        for g in gaps:
            v = float(g)
            placed = False
            for i, th in enumerate(thresholds):
                if v <= th:
                    buckets[i] += 1
                    placed = True
                    break
            if not placed:
                buckets[-1] += 1

        labels = []
        if thresholds:
            labels.append(f"≤{thresholds[0]}%")
            for i in range(1, len(thresholds)):
                labels.append(f"{thresholds[i-1]}–{thresholds[i]}%")
            labels.append(f">{thresholds[-1]}%")
        else:
            labels = ["All"]

        data = [{"name": labels[i], "value": buckets[i]} for i in range(len(labels))]
        return create_response(data=data)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("ceiling_funnel failed")
        raise HTTPException(status_code=500, detail="Internal error in /targets/ceiling/funnel: " + str(e))
