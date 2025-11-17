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

DAILY_SRC = "daily_joined_data"


# -------------------- Helpers --------------------


def _resolve_price_col(adjusted: bool, currency: Literal["rial", "usd"]) -> str:
    """
    ستون قیمت برای محاسبه سقف:
      - ریالی: high یا adjust_high
      - دلاری: high_usd یا adjust_high_usd
    """
    col = "adjust_high" if adjusted else "high"
    if currency == "usd":
        col += "_usd"
    return col


def _safe_num(col: str) -> str:
    """NaN را به NULL تبدیل می‌کند"""
    return f"CASE WHEN {col}::text = 'NaN' THEN NULL ELSE {col} END"


def _not_nan(col: str) -> str:
    """فقط سطرهای غیر NaN"""
    return f"NOT ({col}::text = 'NaN')"


# -------------------- Main Endpoint --------------------


@router.get("/ceiling", summary="Gap-to-Ceiling (ATH یا بازه start/end)")
async def ceiling_targets(
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    sector: Optional[str] = Query(None, description="symboldetail.sector"),
    adjusted: bool = Query(True),
    currency: Literal["rial", "usd"] = Query("rial"),
    _=Depends(require_permissions("Report.Ceiling.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    """
    خروجی: [{
      stock_ticker, price_now, price_j_date,
      ceiling_price, ceiling_j_date,
      gap_abs, gap_pct, hit
    }]
    """
    try:
        price_col = _resolve_price_col(adjusted, currency)

        if end_date and not start_date:
            return create_response(
                status="error",
                status_code=400,
                message="وقتی end_date می‌دهید باید start_date هم بدهید.",
                data=[],
            )

        # -------------------- حالت 1: بازه --------------------
        if start_date:
            sector_join = (
                "JOIN (SELECT DISTINCT stock_ticker FROM symboldetail "
                "WHERE sector = :sector) s USING (stock_ticker)"
                if sector
                else ""
            )

            sql = f"""
            WITH params AS (
              SELECT
                :start_date::date                       AS start_date,
                COALESCE(:end_date::date, CURRENT_DATE) AS end_wish
            ),
            end_anchor AS (
              -- اگر end_wish از آخرین تاریخ دیتابیس جلوتر بود، آن را به max(date_miladi) محدود می‌کنیم
              SELECT LEAST(p.end_wish,
                           (SELECT max(date_miladi) FROM {DAILY_SRC})) AS end_date
              FROM params p
            ),
            base AS (
              -- آخرین قیمت هر نماد در تاریخ end_date (فقط جایی که قیمت NaN نباشد)
              SELECT DISTINCT ON (dj.stock_ticker)
                dj.stock_ticker,
                dj.j_date AS price_j_date,
                {_safe_num(f'dj.{price_col}')} AS price_now
              FROM {DAILY_SRC} dj, end_anchor ea
              WHERE dj.date_miladi = ea.end_date
                AND {_not_nan(f'dj.{price_col}')}
              ORDER BY
                dj.stock_ticker,
                dj.{price_col} DESC,
                dj.date_miladi DESC
            ),
            ranked AS (
              -- پیدا کردن سقف (بیشترین high / adjust_high) در بازه start_date تا end_date
              SELECT
                dj.stock_ticker,
                dj.j_date,
                dj.date_miladi,
                {_safe_num(f'dj.{price_col}')} AS px,
                ROW_NUMBER() OVER (
                  PARTITION BY dj.stock_ticker
                  ORDER BY dj.{price_col} DESC, dj.date_miladi DESC
                ) AS rn
              FROM {DAILY_SRC} dj
              JOIN end_anchor ea ON TRUE
              WHERE dj.date_miladi BETWEEN :start_date::date AND ea.end_date
                AND {_not_nan(f'dj.{price_col}')}
            ),
            wnd AS (
              SELECT stock_ticker, px AS ceiling_price, j_date AS ceiling_j_date
              FROM ranked
              WHERE rn = 1
            )
            SELECT
              b.stock_ticker,
              b.price_now,
              b.price_j_date,
              w.ceiling_price,
              w.ceiling_j_date,
              (w.ceiling_price - b.price_now) AS gap_abs,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (w.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct,
              CASE
                 WHEN w.ceiling_price IS NULL OR b.price_now IS NULL THEN NULL
                 ELSE (b.price_now >= w.ceiling_price - 1e-9)
              END AS hit
            FROM base b
            JOIN wnd  w USING (stock_ticker)
            {sector_join}
            ORDER BY gap_pct DESC NULLS LAST;
            """

            params = {
                "start_date": start_date,
                "end_date": end_date,
            }
            if sector:
                params["sector"] = sector

            q = text(sql).bindparams(**params)
            rows = (await db.execute(q)).mappings().all()
            return create_response(data=[dict(r) for r in rows])

        # -------------------- حالت 2: ATH --------------------
        sector_join = (
            "JOIN (SELECT DISTINCT stock_ticker FROM symboldetail "
            "WHERE sector = :sector) s USING (stock_ticker)"
            if sector
            else ""
        )

        sql = f"""
        WITH lastday AS (
          SELECT max(date_miladi) AS dmax FROM {DAILY_SRC}
        ),
        base AS (
          -- آخرین قیمت هر نماد در آخرین روز دیتابیس (فقط جایی که قیمت NaN نباشد)
          SELECT DISTINCT ON (dj.stock_ticker)
            dj.stock_ticker,
            dj.j_date AS price_j_date,
            {_safe_num(f'dj.{price_col}')} AS price_now
          FROM {DAILY_SRC} dj, lastday ld
          WHERE dj.date_miladi = ld.dmax
            AND {_not_nan(f'dj.{price_col}')}
          ORDER BY
            dj.stock_ticker,
            dj.{price_col} DESC,
            dj.date_miladi DESC
        ),
        ranked AS (
          -- ATH: بیشترین high / adjust_high کل تاریخ
          SELECT
            dj.stock_ticker,
            dj.j_date,
            dj.date_miladi,
            {_safe_num(f'dj.{price_col}')} AS px,
            ROW_NUMBER() OVER (
              PARTITION BY dj.stock_ticker
              ORDER BY dj.{price_col} DESC, dj.date_miladi DESC
            ) AS rn
          FROM {DAILY_SRC} dj
          WHERE {_not_nan(f'dj.{price_col}')}
        ),
        ath AS (
          SELECT stock_ticker, px AS ceiling_price, j_date AS ceiling_j_date
          FROM ranked
          WHERE rn = 1
        )
        SELECT
          b.stock_ticker,
          b.price_now,
          b.price_j_date,
          a.ceiling_price,
          a.ceiling_j_date,
          (a.ceiling_price - b.price_now) AS gap_abs,
          CASE WHEN b.price_now > 0
               THEN 100.0 * (a.ceiling_price - b.price_now)/b.price_now
               ELSE NULL END AS gap_pct,
          CASE
             WHEN a.ceiling_price IS NULL OR b.price_now IS NULL THEN NULL
             ELSE (b.price_now >= a.ceiling_price - 1e-9)
          END AS hit
        FROM base b
        JOIN ath a USING (stock_ticker)
        {sector_join}
        ORDER BY gap_pct DESC NULLS LAST;
        """

        params = {}
        if sector:
            params["sector"] = sector

        q = text(sql).bindparams(**params)
        rows = (await db.execute(q)).mappings().all()
        return create_response(data=[dict(r) for r in rows])

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("ceiling_targets failed")
        raise HTTPException(
            status_code=500,
            detail="Internal error in /targets/ceiling: " + str(e),
        )


# -------------------- Funnel Endpoint --------------------


@router.get("/ceiling/funnel", summary="Funnel buckets of gap-to-ceiling (range-based)")
async def ceiling_funnel(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    adjusted: bool = Query(True),
    currency: Literal["rial", "usd"] = Query("rial"),
    bins: List[float] = Query([1, 2, 5, 10]),
    _=Depends(require_permissions("Report.Ceiling.View", "ALL")),
    db: AsyncSession = Depends(get_db),
):
    try:
        price_col = _resolve_price_col(adjusted, currency)

        if end_date and not start_date:
            return create_response(
                status="error",
                status_code=400,
                message="وقتی end_date می‌دهید باید start_date هم بدهید.",
                data=[],
            )

        if start_date:
            sector_join = (
                "JOIN (SELECT DISTINCT stock_ticker FROM symboldetail "
                "WHERE sector = :sector) s USING (stock_ticker)"
                if sector
                else ""
            )

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
              -- آخرین قیمت هر نماد در end_date (فقط غیر NaN)
              SELECT DISTINCT ON (dj.stock_ticker)
                dj.stock_ticker,
                {_safe_num(f'dj.{price_col}')} AS price_now
              FROM {DAILY_SRC} dj, end_anchor ea
              WHERE dj.date_miladi = ea.end_date
                AND {_not_nan(f'dj.{price_col}')}
              ORDER BY
                dj.stock_ticker,
                dj.{price_col} DESC,
                dj.date_miladi DESC
            ),
            ranked AS (
              -- سقف در بازه
              SELECT
                dj.stock_ticker,
                {_safe_num(f'dj.{price_col}')} AS px,
                ROW_NUMBER() OVER (
                  PARTITION BY dj.stock_ticker
                  ORDER BY dj.{price_col} DESC, dj.date_miladi DESC
                ) AS rn
              FROM {DAILY_SRC} dj
              JOIN end_anchor ea ON TRUE
              WHERE dj.date_miladi BETWEEN :start_date::date AND ea.end_date
                AND {_not_nan(f'dj.{price_col}')}
            ),
            wnd AS (
              SELECT stock_ticker, px AS ceiling_price
              FROM ranked
              WHERE rn = 1
            )
            SELECT
              b.stock_ticker,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (w.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct
            FROM base b
            JOIN wnd  w USING (stock_ticker)
            {sector_join};
            """

            params = {
                "start_date": start_date,
                "end_date": end_date,
            }
            if sector:
                params["sector"] = sector

            q = text(sql).bindparams(**params)
        else:
            sector_join = (
                "JOIN (SELECT DISTINCT stock_ticker FROM symboldetail "
                "WHERE sector = :sector) s USING (stock_ticker)"
                if sector
                else ""
            )

            sql = f"""
            WITH lastday AS (
              SELECT max(date_miladi) AS dmax FROM {DAILY_SRC}
            ),
            base AS (
              -- آخرین قیمت هر نماد در آخرین روز دیتابیس (فقط غیر NaN)
              SELECT DISTINCT ON (dj.stock_ticker)
                dj.stock_ticker,
                {_safe_num(f'dj.{price_col}')} AS price_now
              FROM {DAILY_SRC} dj, lastday ld
              WHERE dj.date_miladi = ld.dmax
                AND {_not_nan(f'dj.{price_col}')}
              ORDER BY
                dj.stock_ticker,
                dj.{price_col} DESC,
                dj.date_miladi DESC
            ),
            ranked AS (
              -- ATH
              SELECT
                dj.stock_ticker,
                {_safe_num(f'dj.{price_col}')} AS px,
                ROW_NUMBER() OVER (
                  PARTITION BY dj.stock_ticker
                  ORDER BY dj.{price_col} DESC, dj.date_miladi DESC
                ) AS rn
              FROM {DAILY_SRC} dj
              WHERE {_not_nan(f'dj.{price_col}')}
            ),
            ath AS (
              SELECT stock_ticker, px AS ceiling_price
              FROM ranked
              WHERE rn = 1
            )
            SELECT
              b.stock_ticker,
              CASE WHEN b.price_now > 0
                   THEN 100.0 * (a.ceiling_price - b.price_now)/b.price_now
                   ELSE NULL END AS gap_pct
            FROM base b
            JOIN ath a USING (stock_ticker)
            {sector_join};
            """

            params = {}
            if sector:
                params["sector"] = sector

            q = text(sql).bindparams(**params)

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

        if thresholds:
            labels = [f"≤{thresholds[0]}%"]
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
        raise HTTPException(
            status_code=500,
            detail="Internal error in /targets/ceiling/funnel: " + str(e),
        )
