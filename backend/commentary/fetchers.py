# backend/commentary/fetchers.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date as dt_date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _rows(result) -> List[Dict[str, Any]]:
    return [dict(r) for r in result.mappings().all()]


def _row1(result) -> Dict[str, Any]:
    r = result.mappings().first()
    return dict(r) if r else {}


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _parse_ts(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    # asyncpg ممکنه str بده
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
    آخرین N ردیف از sector_intraday_snapshot (ممکن است شامل چند ts مختلف باشد)
    """
    q = text(f"""
        SELECT *
        FROM sector_intraday_snapshot
        ORDER BY ts DESC
        LIMIT {int(n)};
    """)
    res = await db.execute(q)
    return _rows(res)


async def fetch_sector_intraday_last_ts(db: AsyncSession) -> Optional[datetime]:
    """
    max(ts) از sector_intraday_snapshot (برای fallback)
    """
    q = text("""SELECT MAX(ts) AS ts FROM sector_intraday_snapshot;""")
    res = await db.execute(q)
    r = res.mappings().first()
    if not r:
        return None
    return _parse_ts(r.get("ts"))


async def fetch_sector_intraday_at_ts(db: AsyncSession, ts: datetime) -> List[Dict[str, Any]]:
    """
    همه ردیف‌های sector_intraday_snapshot در یک ts مشخص
    """
    q = text("""
        SELECT *
        FROM sector_intraday_snapshot
        WHERE ts = :ts
        ORDER BY total_value DESC NULLS LAST;
    """)
    res = await db.execute(q, {"ts": ts})
    return _rows(res)


def _mode(values: List[Any]) -> Optional[Any]:
    if not values:
        return None
    freq: Dict[Any, int] = {}
    for v in values:
        if v is None:
            continue
        freq[v] = freq.get(v, 0) + 1
    if not freq:
        return None
    return sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[0][0]


def _infer_imbalance_state_from_imb5(imb5: Optional[float]) -> Optional[str]:
    if imb5 is None:
        return None
    if imb5 >= 0.20:
        return "buy_pressure"
    if imb5 <= -0.20:
        return "sell_pressure"
    return "balanced"


def _aggregate_market_from_sector_rows(rows: List[Dict[str, Any]], ts: Optional[datetime]) -> Dict[str, Any]:
    """
    ساخت market_snapshot از روی sector rows (best-effort)
    انتظار ستون‌ها (اگر وجود داشته باشند):
      total_value, net_real_value, net_legal_value, symbols_count,
      green_count, red_count, green_ratio,
      eqw_avg_ret_pct,
      imbalance5, imbalance_state
    """
    if not rows:
        return {}

    total_value_sum = 0.0
    net_real_sum = 0.0
    net_legal_sum = 0.0
    symbols_sum = 0

    green_count_sum = 0
    red_count_sum = 0

    imb5_vals: List[float] = []
    imb_state_vals: List[Any] = []
    eqw_vals: List[float] = []
    green_ratio_vals: List[float] = []

    for r in rows:
        tv = _to_float(r.get("total_value"))
        if tv is not None:
            total_value_sum += tv

        nr = _to_float(r.get("net_real_value"))
        if nr is not None:
            net_real_sum += nr

        nl = _to_float(r.get("net_legal_value"))
        if nl is not None:
            net_legal_sum += nl

        sc = _to_int(r.get("symbols_count"))
        if sc is not None:
            symbols_sum += sc

        gc = _to_int(r.get("green_count"))
        rc = _to_int(r.get("red_count"))
        if gc is not None:
            green_count_sum += gc
        if rc is not None:
            red_count_sum += rc

        gr = _to_float(r.get("green_ratio"))
        if gr is not None:
            green_ratio_vals.append(gr)

        eqw = _to_float(r.get("eqw_avg_ret_pct"))
        if eqw is not None:
            eqw_vals.append(eqw)

        imb5 = _to_float(r.get("imbalance5"))
        if imb5 is not None:
            imb5_vals.append(imb5)

        st = r.get("imbalance_state") or r.get("imbalanceState") or r.get("IMBALANCE_STATE")
        if st is not None:
            imb_state_vals.append(st)

    # green_ratio: اولویت با (green_count/red_count)، بعد میانگین green_ratio
    green_ratio: Optional[float] = None
    if (green_count_sum + red_count_sum) > 0:
        green_ratio = green_count_sum / float(green_count_sum + red_count_sum)
    elif green_ratio_vals:
        green_ratio = sum(green_ratio_vals) / float(len(green_ratio_vals))

    eqw_avg: Optional[float] = None
    if eqw_vals:
        eqw_avg = sum(eqw_vals) / float(len(eqw_vals))

    imb5_avg: Optional[float] = None
    if imb5_vals:
        imb5_avg = sum(imb5_vals) / float(len(imb5_vals))

    imb_state = _mode(imb_state_vals)
    if imb_state is None:
        imb_state = _infer_imbalance_state_from_imb5(imb5_avg)

    out = {
        "ts": ts,
        "snapshot_day": _snapshot_day_from_ts(ts),
        "total_value": int(round(total_value_sum)) if total_value_sum != 0 else None,
        "net_real_value": int(round(net_real_sum)) if net_real_sum != 0 else None,
        "net_legal_value": int(round(net_legal_sum)) if net_legal_sum != 0 else None,
        "symbols_count": symbols_sum if symbols_sum != 0 else None,
        "green_ratio": green_ratio,
        "eqw_avg_ret_pct": eqw_avg,
        "imbalance5": imb5_avg,
        "imbalance_state": imb_state,
        "_source": "sector_intraday_fallback",
    }

    # حذف کلیدهای None برای تمیزی
    return {k: v for k, v in out.items() if v is not None}


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

    ✅ بهبود: اگر market_intraday_snapshot خالی بود،
       از sector_intraday_snapshot آخرین ts را پیدا می‌کنیم
       و market_snapshot را به صورت تجمیعی می‌سازیم.
    """
    # --- daily
    sector_daily = await fetch_sector_daily_latest(db)
    sector_rs = await fetch_sector_rs_latest(db)
    market_daily = await fetch_market_daily_latest(db)

    # --- intraday primary
    market_intraday = await fetch_market_intraday_last(db)
    sector_intraday_lastn = await fetch_sector_intraday_last_n(db, n=sector_snapshot_limit)

    daily_date = sector_daily[0].get("date_miladi") if sector_daily else None

    intraday_ts = _parse_ts(market_intraday.get("ts")) if market_intraday else None
    intraday_day = market_intraday.get("snapshot_day") if market_intraday else None

    market_snapshot_final = market_intraday if market_intraday else {}
    sector_snapshots_final = sector_intraday_lastn or []

    # --- fallback if market intraday is empty
    if not intraday_ts:
        ts2 = await fetch_sector_intraday_last_ts(db)
        if ts2:
            rows_at_ts = await fetch_sector_intraday_at_ts(db, ts2)

            # اگر lastn خالی بود، حالا حداقل چندتا row داریم
            if not sector_snapshots_final:
                sector_snapshots_final = rows_at_ts[:sector_snapshot_limit]

            # ساخت market_snapshot تجمیعی
            market_snapshot_final = _aggregate_market_from_sector_rows(rows_at_ts, ts2)

            intraday_ts = ts2
            intraday_day = _snapshot_day_from_ts(ts2)

    return {
        "daily": {
            "asof": {"date_miladi": daily_date},
            "sector_daily_latest": sector_daily,
            "sector_rs_latest": sector_rs,
            "market_daily_latest": market_daily,
        },
        "intraday": {
            "asof": {"ts": intraday_ts, "snapshot_day": intraday_day},
            "market_snapshot": market_snapshot_final,
            "sector_snapshots": sector_snapshots_final,
        },
    }