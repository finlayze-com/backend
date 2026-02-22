# backend/commentary/fetchers.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _rows(result) -> List[Dict[str, Any]]:
    return [dict(r) for r in result.mappings().all()]


def _row1(result) -> Dict[str, Any]:
    r = result.mappings().first()
    return dict(r) if r else {}


# ----------------------------
# Public fetch API
# ----------------------------

async def fetch_sector_daily_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    آخرین روز موجود در mv_sector_daily_latest را می‌گیرد (همه sectorها در همان روز).
    """
    q = text("""
        SELECT *
        FROM mv_sector_daily_latest
        WHERE date_miladi = (SELECT MAX(date_miladi) FROM mv_sector_daily_latest)
        ORDER BY total_value DESC;
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_sector_rs_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    آخرین روز موجود در mv_sector_relative_strength را می‌گیرد.
    """
    q = text("""
        SELECT *
        FROM mv_sector_relative_strength
        WHERE date_miladi = (SELECT MAX(date_miladi) FROM mv_sector_relative_strength)
        ORDER BY rs_20d DESC;
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_market_daily_latest(db: AsyncSession) -> List[Dict[str, Any]]:
    """
    mv_market_daily_latest معمولاً فقط یک روز را دارد (آخرین روز).
    """
    q = text("""SELECT * FROM mv_market_daily_latest;""")
    res = await db.execute(q)
    return _rows(res)


async def fetch_market_intraday_last(db: AsyncSession) -> Dict[str, Any]:
    """
    آخرین snapshot بازار از market_intraday_snapshot
    """
    q = text("""
        SELECT *
        FROM market_intraday_snapshot
        ORDER BY ts DESC
        LIMIT 1;
    """)
    res = await db.execute(q)
    return _row1(res)


async def fetch_sector_intraday_last_n(db: AsyncSession, n: int = 10) -> List[Dict[str, Any]]:
    """
    آخرین N ردیف از sector_intraday_snapshot
    (برای debug/نمودار/LLM مفید است)
    """
    q = text(f"""
        SELECT *
        FROM sector_intraday_snapshot
        ORDER BY ts DESC
        LIMIT {int(n)};
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_facts_bundle(
    db: AsyncSession,
    *,
    sector_snapshot_limit: int = 10,
) -> Dict[str, Any]:
    """
    خروجی خام برای signals.py:
    facts = {
      "daily": {...},
      "intraday": {...}
    }
    """
    sector_daily = await fetch_sector_daily_latest(db)
    sector_rs = await fetch_sector_rs_latest(db)
    market_daily = await fetch_market_daily_latest(db)

    market_intraday = await fetch_market_intraday_last(db)
    sector_intraday = await fetch_sector_intraday_last_n(db, n=sector_snapshot_limit)

    # asof extraction (best-effort)
    daily_date = sector_daily[0].get("date_miladi") if sector_daily else None
    intraday_ts = market_intraday.get("ts") if market_intraday else None
    intraday_day = market_intraday.get("snapshot_day") if market_intraday else None

    return {
        "daily": {
            "asof": {"date_miladi": daily_date},
            "sector_daily_latest": sector_daily,
            "sector_rs_latest": sector_rs,
            "market_daily_latest": market_daily,
        },
        "intraday": {
            "asof": {"ts": intraday_ts, "snapshot_day": intraday_day},
            "market_snapshot": market_intraday,
            "sector_snapshots": sector_intraday,
        },
    }