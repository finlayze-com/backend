# backend/commentary/signals.py
# -*- coding: utf-8 -*-
"""
Rule-based signals + scoring (deterministic)

هدف:
- ورودی: facts (خروجی fetchers از MV ها + snapshot ها)
- خروجی: signals استاندارد برای narrative.py
- ساختار طوریه که بعداً LLM هم بتونه همین signals رو override/augment کنه.

نکته: این فایل هیچ SQL نداره. فقط منطق/قواعد.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from math import isfinite


# ----------------------------
# Helpers
# ----------------------------

def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not isfinite(v):
            return None
        return v
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _norm_green_ratio(x: Optional[float]) -> Optional[float]:
    """
    بعضی وقت‌ها green_ratio را 0..1 داریم، بعضی وقت‌ها 0..100.
    این تابع آن را به 0..1 تبدیل می‌کند.
    """
    if x is None:
        return None
    if x > 1.5:  # یعنی احتمالاً درصدی است
        return x / 100.0
    return x


def _clip01(x: float) -> float:
    if x < 0:
        return 0.0
    if x > 1:
        return 1.0
    return float(x)


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _abs(x: Optional[float]) -> float:
    return abs(x) if x is not None else 0.0


# ----------------------------
# Evidence reference helpers
# ----------------------------

def _ref(*paths: str) -> List[str]:
    return [p for p in paths if p]


def _anomaly(code: str, text: str, evidence: List[str], severity: str = "warn") -> Dict[str, Any]:
    return {
        "code": code,
        "text": text,
        "severity": severity,
        "evidence_refs": evidence,
    }


# ----------------------------
# Public API
# ----------------------------

def build_signals(facts: Dict[str, Any]) -> Dict[str, Any]:
    """
    facts structure پیشنهادی از fetchers.py:

    facts = {
      "daily": {
        "asof": {"date_miladi": "2026-02-??"},
        "sector_daily_latest": [ {sector,total_value,net_real_value, ...}, ... ],  # from mv_sector_daily_latest
        "sector_rs_latest": [ {sector, rs_20d, sector_ret_1d, market_ret_1d, ...}, ... ], # from mv_sector_relative_strength latest day
        "market_daily_latest": [ {market,total_value, ...}, ... ],  # mv_market_daily_latest
      },
      "intraday": {
        "asof": {"ts": "...", "snapshot_day": "..."},
        "market_snapshot": {symbols_count, green_ratio, eqw_avg_ret_pct, total_value, net_real_value, imbalance5, imbalance_state, ...}, # market_intraday_snapshot last row
        "sector_snapshots": [ {sector_key, total_value, net_real_value, imbalance5, imbalance_state, ...}, ... ] # sector_intraday_snapshot last N rows
      }
    }
    """

    daily = (facts or {}).get("daily") or {}
    intraday = (facts or {}).get("intraday") or {}

    # --- Intraday snapshot (market) ---
    mshot = (intraday.get("market_snapshot") or {})
    intraday_green = _norm_green_ratio(_to_float(mshot.get("green_ratio")))
    intraday_eqw = _to_float(mshot.get("eqw_avg_ret_pct"))
    intraday_total_value = _to_int(mshot.get("total_value"))
    intraday_net_real = _to_int(mshot.get("net_real_value"))
    intraday_net_legal = _to_int(mshot.get("net_legal_value"))
    intraday_imb5 = _to_float(mshot.get("imbalance5"))
    intraday_imb_state = (mshot.get("imbalance_state") or mshot.get("imbalance_state".upper()) or mshot.get("imbalanceState"))
    if intraday_imb_state is None:
        intraday_imb_state = mshot.get("imbalance_state")  # just in case

    # --- Daily latest by sector ---
    sec_daily_rows = daily.get("sector_daily_latest") or []
    # --- Daily RS latest ---
    sec_rs_rows = daily.get("sector_rs_latest") or []

    # --- ETF buckets extracted from sector_daily_latest rows ---
    etf_buckets = _extract_etf_buckets(sec_daily_rows)

    # --- Leaders from RS and money flow ---
    leaders = _leaders_from_daily(sec_rs_rows, sec_daily_rows)

    # --- Market state classification ---
    market_state, anomalies = _classify_market_state(
        intraday_green=intraday_green,
        intraday_eqw=intraday_eqw,
        intraday_net_real=intraday_net_real,
        intraday_imb5=intraday_imb5,
        intraday_imb_state=intraday_imb_state,
        leaders=leaders,
    )

    # attach ETF availability
    etf_signals = {
        "available": bool(etf_buckets),
        "buckets": etf_buckets,
    }

    # final bundle
    return {
        "market_state": market_state,
        "leaders": leaders,
        "etf": etf_signals,
        "anomalies": anomalies,
    }


# ----------------------------
# ETF extraction
# ----------------------------

_ETF_PREFIX = "صندوق سرمایه گذاری قابل معامله |"

def _is_etf_sector(sector_name: str) -> bool:
    return isinstance(sector_name, str) and sector_name.strip().startswith(_ETF_PREFIX)

def _etf_bucket_name(sector_name: str) -> str:
    # "صندوق سرمایه گذاری قابل معامله | طلا" -> "طلا"
    try:
        return sector_name.split("|", 1)[1].strip()
    except Exception:
        return "other"

def _extract_etf_buckets(sector_daily_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    از mv_sector_daily_latest می‌گیرد.
    انتظار: sector ها شامل:
      "صندوق سرمایه گذاری قابل معامله | طلا"
      ...
    """
    buckets: List[Dict[str, Any]] = []

    for r in sector_daily_rows or []:
        s = (r.get("sector") or "").strip()
        if not _is_etf_sector(s):
            continue
        buckets.append({
            "bucket": _etf_bucket_name(s),
            "sector": s,
            "symbols_count": _to_int(r.get("symbols_count")),
            "total_value": _to_float(r.get("total_value")),
            "total_volume": _to_float(r.get("total_volume")),
            "marketcap": _to_float(r.get("marketcap")),
            "net_real_value": _to_float(r.get("net_real_value")),
            # اگر بعداً RS ETF را هم اضافه کردی، اینجا merge می‌کنیم
        })

    # sort by total_value desc
    buckets.sort(key=lambda x: float(x.get("total_value") or 0), reverse=True)
    return buckets


# ----------------------------
# Leaders (RS + money)
# ----------------------------

def _leaders_from_daily(
    rs_rows: List[Dict[str, Any]],
    daily_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    خروجی:
    {
      "rs20_top": [ {sector, rs_20d, ...}, ... ],
      "rs20_bottom": [...],
      "money_in_top": [ {sector, net_real_value, ...}, ...],
      "money_out_top": [...]
    }
    """

    # --- RS leaders: ignore ETF in market RS? نه، اینجا RS هر sector که در mv_sector_relative_strength هست را می‌گیریم.
    # اگر بعداً RS را برای ETF هم ساختی، این لیست هم ETF را خواهد داشت.
    cleaned_rs = []
    for r in rs_rows or []:
        sec = (r.get("sector") or "").strip()
        rs20 = _to_float(r.get("rs_20d"))
        if not sec or rs20 is None:
            continue
        cleaned_rs.append({
            "sector": sec,
            "rs_20d": rs20,
            "sector_ret_1d": _to_float(r.get("sector_ret_1d")),
            "market_ret_1d": _to_float(r.get("market_ret_1d")),
        })

    rs_sorted = sorted(cleaned_rs, key=lambda x: x["rs_20d"], reverse=True)
    rs_top = rs_sorted[:5]
    rs_bottom = list(reversed(rs_sorted[-5:])) if len(rs_sorted) >= 5 else list(reversed(rs_sorted))

    # --- Money leaders from mv_sector_daily_latest (net_real_value)
    money = []
    for r in daily_rows or []:
        sec = (r.get("sector") or "").strip()
        nrv = _to_float(r.get("net_real_value"))
        if not sec or nrv is None:
            continue
        money.append({"sector": sec, "net_real_value": nrv, "total_value": _to_float(r.get("total_value"))})

    money_in = sorted([m for m in money if (m["net_real_value"] or 0) > 0], key=lambda x: x["net_real_value"], reverse=True)[:5]
    money_out = sorted([m for m in money if (m["net_real_value"] or 0) < 0], key=lambda x: x["net_real_value"])[:5]

    return {
        "rs20_top": rs_top,
        "rs20_bottom": rs_bottom,
        "money_in_top": money_in,
        "money_out_top": money_out,
    }


# ----------------------------
# Market state classification
# ----------------------------

def _classify_market_state(
    *,
    intraday_green: Optional[float],
    intraday_eqw: Optional[float],
    intraday_net_real: Optional[int],
    intraday_imb5: Optional[float],
    intraday_imb_state: Optional[str],
    leaders: Dict[str, Any],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    خروجی market_state:
    {
      "breadth": {"green_ratio":..., "eqw_avg_ret_pct":...},
      "flow": {"net_real_value":..., "net_legal_value":...},
      "orderbook": {"imbalance5":..., "state":...},
      "trend": "bullish/bearish/mixed",
      "regime": "risk_on/risk_off/neutral",
      "confidence": 0..1
    }

    + anomalies
    """

    anomalies: List[Dict[str, Any]] = []

    gr = intraday_green
    eqw = intraday_eqw
    nrv = intraday_net_real
    imb5 = intraday_imb5

    # --- Simple scoring ---
    score = 0.0
    weight_sum = 0.0

    # Breadth contribution
    if gr is not None:
        # <0.35 bearish, 0.35..0.60 mixed, >0.60 bullish
        if gr < 0.35:
            score -= 1.0
        elif gr > 0.60:
            score += 1.0
        weight_sum += 1.0

    # Flow contribution (sign only; magnitude thresholds later)
    if nrv is not None:
        if nrv > 0:
            score += 1.0
        elif nrv < 0:
            score -= 1.0
        weight_sum += 1.0

    # Orderbook contribution
    if intraday_imb_state:
        st = str(intraday_imb_state).lower()
        if "bull" in st:
            score += 0.7
            weight_sum += 0.7
        elif "bear" in st:
            score -= 0.7
            weight_sum += 0.7
        else:
            weight_sum += 0.3  # neutral gives some certainty
    elif imb5 is not None:
        # fallback: imbalance5 numeric
        if imb5 > 0.15:
            score += 0.7
        elif imb5 < -0.15:
            score -= 0.7
        weight_sum += 0.7

    # If we have no inputs, return neutral low confidence
    if weight_sum == 0:
        return ({
            "breadth": {"green_ratio": None, "eqw_avg_ret_pct": None},
            "flow": {"net_real_value": None},
            "orderbook": {"imbalance5": None, "state": None},
            "trend": "mixed",
            "regime": "neutral",
            "confidence": 0.4,
        }, anomalies)

    norm_score = score / weight_sum  # roughly -1..+1

    if norm_score > 0.25:
        trend = "bullish"
    elif norm_score < -0.25:
        trend = "bearish"
    else:
        trend = "mixed"

    # Regime: risk_on/off using stronger conditions (breadth + flow align)
    if gr is not None and nrv is not None:
        if gr > 0.55 and nrv > 0:
            regime = "risk_on"
        elif gr < 0.40 and nrv < 0:
            regime = "risk_off"
        else:
            regime = "neutral"
    else:
        regime = "neutral"

    # Confidence: based on how many signals available + how aligned
    # alignment: abs(norm_score) higher -> higher confidence
    available = 0
    for v in [gr, nrv, intraday_imb_state or imb5]:
        if v is not None:
            available += 1

    base_conf = 0.45 + 0.15 * available  # 1->0.60, 2->0.75, 3->0.90
    align_boost = min(0.10, 0.10 * abs(norm_score))  # up to 0.10
    confidence = _clip01(base_conf + align_boost)

    # --- Anomaly rules ---
    # 1) breadth bullish but net real negative (distribution risk)
    if gr is not None and nrv is not None:
        if gr > 0.60 and nrv < 0:
            anomalies.append(_anomaly(
                "BREADTH_POS_BUT_REAL_OUTFLOW",
                "پهنای بازار مثبت است اما خروج پول حقیقی دیده می‌شود (احتمالاً رالی بدون حمایت پول یا چرخش درون‌گروهی).",
                _ref("intraday.market_snapshot.green_ratio", "intraday.market_snapshot.net_real_value"),
                severity="warn",
            ))
        if gr < 0.35 and nrv > 0:
            anomalies.append(_anomaly(
                "BREADTH_NEG_BUT_REAL_INFLOW",
                "پهنای بازار منفی است اما ورود پول حقیقی دیده می‌شود (ممکن است خرید حمایتی روی چند گروه خاص باشد).",
                _ref("intraday.market_snapshot.green_ratio", "intraday.market_snapshot.net_real_value"),
                severity="warn",
            ))

    # 2) orderbook bullish but breadth very low (fake bids / late strength)
    if intraday_imb_state:
        st = str(intraday_imb_state).lower()
        if ("bull" in st) and (gr is not None and gr < 0.35):
            anomalies.append(_anomaly(
                "ORDERBOOK_BULL_BUT_WEAK_BREADTH",
                "دفتر سفارش‌ها متمایل به خرید است اما پهنای بازار ضعیف است (ممکن است سفارش‌ها نمایشی یا محدود به چند نماد باشد).",
                _ref("intraday.market_snapshot.imbalance_state", "intraday.market_snapshot.green_ratio"),
                severity="warn",
            ))
        if ("bear" in st) and (gr is not None and gr > 0.60):
            anomalies.append(_anomaly(
                "ORDERBOOK_BEAR_BUT_STRONG_BREADTH",
                "دفتر سفارش‌ها متمایل به فروش است اما پهنای بازار قوی است (ممکن است فشار فروش در دقایق پایانی/چند نماد بزرگ باشد).",
                _ref("intraday.market_snapshot.imbalance_state", "intraday.market_snapshot.green_ratio"),
                severity="warn",
            ))

    market_state = {
        "breadth": {
            "green_ratio": gr,
            "eqw_avg_ret_pct": eqw,
        },
        "flow": {
            "net_real_value": nrv,
        },
        "orderbook": {
            "imbalance5": imb5,
            "state": intraday_imb_state,
        },
        "trend": trend,
        "regime": regime,
        "confidence": confidence,
    }

    return market_state, anomalies