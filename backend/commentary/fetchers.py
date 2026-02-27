# backend/commentary/fetchers.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, date as dt_date, time as dt_time, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


# ----------------------------
# Helpers
# ----------------------------

def _rows(result) -> List[Dict[str, Any]]:
    return [dict(r) for r in result.mappings().all()]


def _row1(result) -> Dict[str, Any]:
    r = result.mappings().first()
    return dict(r) if r else {}


def _parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


def _snapshot_day_from_ts(ts: Optional[datetime]) -> Optional[dt_date]:
    if ts is None:
        return None
    try:
        return ts.date()
    except Exception:
        return None


def _date_bounds(day: dt_date) -> tuple[datetime, datetime]:
    """
    بازه‌ی روز (از 00:00 تا 23:59:59) — برای سری intraday.
    اگر timezone-aware هستی بهتره اینجا TZ هم اعمال شود.
    """
    start = datetime.combine(day, dt_time(0, 0, 0))
    end = start + timedelta(days=1)
    return start, end


# ----------------------------
# Daily fetch
# ----------------------------

async def fetch_sector_daily_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM mv_sector_daily_latest
        WHERE date_miladi = (SELECT MAX(date_miladi) FROM mv_sector_daily_latest)
        ORDER BY total_value DESC;
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_sector_rs_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM mv_sector_relative_strength
        WHERE date_miladi = (SELECT MAX(date_miladi) FROM mv_sector_relative_strength)
        ORDER BY rs_20d DESC;
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_sector_baseline_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM mv_sector_baseline
        WHERE date_miladi = (SELECT MAX(date_miladi) FROM mv_sector_baseline)
        ORDER BY total_value DESC NULLS LAST;
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_market_daily_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    q = text("""SELECT * FROM mv_market_daily_latest;""")
    res = await db.execute(q)
    return _rows(res)


# ----------------------------
# Intraday: latest snapshot
# ----------------------------

async def fetch_market_intraday_last(db: AsyncSession) -> Dict[str, Any]:
    q = text("""
        SELECT *
        FROM market_intraday_snapshot
        ORDER BY ts DESC
        LIMIT 1;
    """)
    res = await db.execute(q)
    return _row1(res)


async def fetch_sector_intraday_last_ts(db: AsyncSession) -> Optional[datetime]:
    q = text("""SELECT MAX(ts) AS ts FROM sector_intraday_snapshot;""")
    res = await db.execute(q)
    r = res.mappings().first()
    return _parse_ts(r.get("ts")) if r else None


async def fetch_sector_intraday_at_ts(db: AsyncSession, ts: datetime, limit: int = 1000) -> List[Dict[str, Any]]:
    q = text(f"""
        SELECT *
        FROM sector_intraday_snapshot
        WHERE ts = :ts
        ORDER BY total_value DESC NULLS LAST
        LIMIT {int(limit)};
    """)
    res = await db.execute(q, {"ts": ts})
    return _rows(res)


# ----------------------------
# Intraday: history series (for morning_story)
# ----------------------------

async def fetch_market_intraday_series(db: AsyncSession, day: dt_date, limit: int = 2000) -> List[Dict[str, Any]]:
    """
    کل روز (یا تا limit) از market_intraday_snapshot برای روایت.
    """
    start, end = _date_bounds(day)
    q = text(f"""
        SELECT *
        FROM market_intraday_snapshot
        WHERE ts >= :start AND ts < :end
        ORDER BY ts ASC
        LIMIT {int(limit)};
    """)
    res = await db.execute(q, {"start": start, "end": end})
    return _rows(res)


async def fetch_sector_intraday_series(db: AsyncSession, day: dt_date, limit: int = 20000) -> List[Dict[str, Any]]:
    """
    کل روز از sector_intraday_snapshot (برای rotation صنایع)
    ممکنه بزرگ بشه؛ limit گذاشتیم.
    """
    start, end = _date_bounds(day)
    q = text(f"""
        SELECT *
        FROM sector_intraday_snapshot
        WHERE ts >= :start AND ts < :end
        ORDER BY ts ASC
        LIMIT {int(limit)};
    """)
    res = await db.execute(q, {"start": start, "end": end})
    return _rows(res)


# ----------------------------
# Intraday MVs (optional but recommended)
# ----------------------------

async def fetch_live_sector_report_last_ts(db: AsyncSession) -> Optional[datetime]:
    q = text("""SELECT MAX(ts) AS ts FROM mv_live_sector_report;""")
    res = await db.execute(q)
    r = res.mappings().first()
    return _parse_ts(r.get("ts")) if r else None


async def fetch_live_sector_report_at_ts(db: AsyncSession, ts: datetime) -> List[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM mv_live_sector_report
        WHERE ts = :ts
        ORDER BY sort_order ASC NULLS LAST;
    """)
    res = await db.execute(q, {"ts": ts})
    return _rows(res)


async def fetch_orderbook_report_last_ts(db: AsyncSession) -> Optional[datetime]:
    q = text("""SELECT MAX(ts) AS ts FROM mv_orderbook_report;""")
    res = await db.execute(q)
    r = res.mappings().first()
    return _parse_ts(r.get("ts")) if r else None


async def fetch_orderbook_report_at_ts(db: AsyncSession, ts: datetime) -> List[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM mv_orderbook_report
        WHERE ts = :ts
        ORDER BY orderbook_total_value DESC NULLS LAST;
    """)
    res = await db.execute(q, {"ts": ts})
    return _rows(res)


# ----------------------------
# Bundle
# ----------------------------

async def fetch_facts_bundle(
    db: AsyncSession,
    *,
    sector_universe_limit: int = 1000,
    market_series_limit: int = 2000,
    sector_series_limit: int = 20000,
) -> Dict[str, Any]:
    """
    facts = {
      "daily": {...},
      "intraday": {...}
    }
    """

    # ---- daily
    sector_daily = await fetch_sector_daily_latest(db)
    sector_rs = await fetch_sector_rs_latest(db)
    sector_baseline = await fetch_sector_baseline_latest(db)
    market_daily = await fetch_market_daily_latest(db)

    daily_date = sector_daily[0].get("date_miladi") if sector_daily else None

    # ---- intraday: latest
    market_intraday = await fetch_market_intraday_last(db)
    intraday_ts = _parse_ts(market_intraday.get("ts")) if market_intraday else None

    if not intraday_ts:
        intraday_ts = await fetch_sector_intraday_last_ts(db)

    intraday_day = _snapshot_day_from_ts(intraday_ts)

    sector_rows_at_ts: List[Dict[str, Any]] = []
    if intraday_ts:
        sector_rows_at_ts = await fetch_sector_intraday_at_ts(db, intraday_ts, limit=sector_universe_limit)

    # ---- intraday: series (for timeline)
    market_series: List[Dict[str, Any]] = []
    sector_series: List[Dict[str, Any]] = []
    if intraday_day:
        market_series = await fetch_market_intraday_series(db, intraday_day, limit=market_series_limit)
        sector_series = await fetch_sector_intraday_series(db, intraday_day, limit=sector_series_limit)

    # ---- intraday MVs
    live_ts = await fetch_live_sector_report_last_ts(db)
    live_rows = await fetch_live_sector_report_at_ts(db, live_ts) if live_ts else []

    ob_ts = await fetch_orderbook_report_last_ts(db)
    ob_rows = await fetch_orderbook_report_at_ts(db, ob_ts) if ob_ts else []

    return {
        "daily": {
            "asof": {"date_miladi": daily_date},
            "sector_daily_latest": sector_daily,
            "sector_rs_latest": sector_rs,
            "sector_baseline_latest": sector_baseline,
            "market_daily_latest": market_daily,
        },
        "intraday": {
            "asof": {"ts": intraday_ts, "snapshot_day": intraday_day},
            "market_snapshot": market_intraday or {},
            "sector_rows_at_ts": sector_rows_at_ts,
            "market_series": market_series,     # ✅ نگه داشتیم برای بعد
            "sector_series": sector_series,     # ✅ نگه داشتیم برای بعد
            "mv_live_sector_report": {"ts": live_ts, "rows": live_rows},
            "mv_orderbook_report": {"ts": ob_ts, "rows": ob_rows},
        },
    }