# backend/commentary/signals.py
# -*- coding: utf-8 -*-
"""
Deterministic text-engine signals (Rulebook-driven)

Output is LLM-friendly:
- compact signals for UI
- explainable (evidence_refs)
- extendable (sector_cards for pro / symbol-level later)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from math import isfinite


# ----------------------------
# helpers
# ----------------------------

def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not isfinite(v):
            return None
        return v
    except Exception:
        return None


def _i(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


def _norm_green_ratio(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    if x > 1.5:
        return x / 100.0
    return x


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _abs(x: Optional[float]) -> float:
    return abs(x) if x is not None else 0.0


def _ref(*paths: str) -> List[str]:
    return [p for p in paths if p]


def _anomaly(code: str, text: str, evidence: List[str], severity: str = "warn") -> Dict[str, Any]:
    return {"code": code, "text": text, "severity": severity, "evidence_refs": evidence}


def _top(rows: List[Dict[str, Any]], key: str, n: int = 5, desc: bool = True) -> List[Dict[str, Any]]:
    xs = []
    for r in rows or []:
        v = _f(r.get(key))
        if v is None:
            continue
        xs.append((v, r))
    xs.sort(key=lambda t: t[0], reverse=desc)
    return [t[1] for t in xs[:n]]

def _to_float(x):
    return _f(x)

def _to_int(x):
    return _i(x)

# ----------------------------
# Buckets (Rulebook)
# ----------------------------

def _bucket_rs20(rs20: Optional[float]) -> str:
    if rs20 is None:
        return "unknown"
    if rs20 >= 0.05:
        return "strong_leader"
    if rs20 > 0:
        return "mild_leader"
    if -0.03 <= rs20 <= 0.03:
        return "neutral"
    if rs20 <= -0.05:
        return "laggard"
    return "neutral"


def _bucket_flow_share(net_real_share: Optional[float]) -> str:
    # share is fraction (0.05 = 5%)
    if net_real_share is None:
        return "unknown"
    if net_real_share >= 0.05:
        return "strong_inflow"
    if 0.01 <= net_real_share < 0.05:
        return "mild_inflow"
    if net_real_share <= -0.05:
        return "outflow"
    return "neutral_flow"


def _bucket_z(z: Optional[float]) -> str:
    """
    magnitude bucket only (sign separately)
    Highly abnormal: |z|>=2
    Abnormal: 1.5<=|z|<2
    Slight: 1<=|z|<1.5
    Normal: else
    """
    if z is None:
        return "unknown"
    az = abs(z)
    if az >= 2.0:
        return "highly_abnormal"
    if az >= 1.5:
        return "abnormal"
    if az >= 1.0:
        return "slight"
    return "normal"


def _z_sign(z: Optional[float]) -> str:
    if z is None:
        return "unknown"
    if z > 0:
        return "pos"
    if z < 0:
        return "neg"
    return "flat"


# ----------------------------
# Market overview (intraday)
# ----------------------------

def _market_overview_from_live(facts: Dict[str, Any]) -> Dict[str, Any]:
    live = ((facts.get("intraday") or {}).get("mv_live_sector_report") or {})
    rows = live.get("rows") or []
    # انتظار: level/key برای market وجود دارد (طبق فایل ستون‌ها)
    market_row = None
    for r in rows:
        lvl = (r.get("level") or "").strip().lower()
        key = (r.get("key") or "").strip().lower()
        if lvl in ("market", "mkt") or key in ("all", "market"):
            market_row = r
            break

    if not market_row:
        return {}

    gr = _norm_green_ratio(_f(market_row.get("green_ratio")))
    out = {
        "ts": live.get("ts"),
        "total_value": _i(market_row.get("total_value")),
        "total_volume": _i(market_row.get("total_volume")),
        "green_ratio": gr,
        "eqw_avg_ret_pct": _f(market_row.get("eqw_avg_ret_pct")),
        "net_real_value": _i(market_row.get("net_real_value")),
        "net_legal_value": _i(market_row.get("net_legal_value")),
        "symbols_count": _i(market_row.get("symbols_count")),
        "_source": "mv_live_sector_report",
    }
    return {k: v for k, v in out.items() if v is not None}


def _market_overview_from_snapshot(facts: Dict[str, Any]) -> Dict[str, Any]:
    m = ((facts.get("intraday") or {}).get("market_snapshot") or {})
    if not m:
        return {}
    out = {
        "ts": m.get("ts"),
        "total_value": _i(m.get("total_value")),
        "total_volume": _i(m.get("total_volume")),
        "green_ratio": _norm_green_ratio(_f(m.get("green_ratio"))),
        "eqw_avg_ret_pct": _f(m.get("eqw_avg_ret_pct")),
        "net_real_value": _i(m.get("net_real_value")),
        "net_legal_value": _i(m.get("net_legal_value")),
        "symbols_count": _i(m.get("symbols_count")),
        "imbalance5": _f(m.get("imbalance5")),
        "imbalance_state": m.get("imbalance_state") or m.get("imbalanceState"),
        "_source": "market_intraday_snapshot",
    }
    return {k: v for k, v in out.items() if v is not None}


def _classify_market_trend(gr: Optional[float], net_real: Optional[int], eqw: Optional[float]) -> Tuple[str, str, float]:
    """
    trend: bullish/bearish/mixed
    regime: risk_on/risk_off/neutral
    confidence: 0..1
    """
    score = 0.0
    w = 0.0

    if gr is not None:
        if gr > 0.60:
            score += 1.0
        elif gr < 0.35:
            score -= 1.0
        w += 1.0

    if net_real is not None:
        if net_real > 0:
            score += 1.0
        elif net_real < 0:
            score -= 1.0
        w += 1.0

    if eqw is not None:
        if eqw > 0.2:
            score += 0.5
        elif eqw < -0.2:
            score -= 0.5
        w += 0.5

    if w == 0:
        return "mixed", "neutral", 0.4

    ns = score / w
    if ns > 0.25:
        trend = "bullish"
    elif ns < -0.25:
        trend = "bearish"
    else:
        trend = "mixed"

    if gr is not None and net_real is not None:
        if gr > 0.55 and net_real > 0:
            regime = "risk_on"
        elif gr < 0.40 and net_real < 0:
            regime = "risk_off"
        else:
            regime = "neutral"
    else:
        regime = "neutral"

    conf = min(0.92, 0.55 + 0.12 * (1 if gr is not None else 0) + 0.12 * (1 if net_real is not None else 0) + 0.08 * (1 if eqw is not None else 0) + 0.05 * abs(ns))
    return trend, regime, conf


# ----------------------------
# Intraday active sectors
# ----------------------------

def _active_sectors_from_intraday_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    خروجی برای narrative:
      top_value_sectors, top_volume_sectors, inflow_sectors, outflow_sectors
    """
    cleaned = []
    for r in rows or []:
        sec = (r.get("sector") or r.get("sector_key") or r.get("key") or "").strip()
        if not sec:
            continue
        cleaned.append({
            "sector": sec,
            "total_value": _f(r.get("total_value")),
            "total_volume": _f(r.get("total_volume")),
            "net_real_value": _f(r.get("net_real_value")),
        })

    top_value = _top(cleaned, "total_value", n=5, desc=True)
    top_vol = _top(cleaned, "total_volume", n=5, desc=True)

    inflow = sorted([x for x in cleaned if (x.get("net_real_value") or 0) > 0],
                    key=lambda x: float(x.get("net_real_value") or 0), reverse=True)[:5]
    outflow = sorted([x for x in cleaned if (x.get("net_real_value") or 0) < 0],
                     key=lambda x: float(x.get("net_real_value") or 0))[:5]

    return {
        "top_value_sectors": top_value,
        "top_volume_sectors": top_vol,
        "inflow_sectors": inflow,
        "outflow_sectors": outflow,
    }


# ----------------------------
# Orderbook rules (mv_orderbook_report)
# ----------------------------

def _orderbook_signals(ob_rows: List[Dict[str, Any]], intraday_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Rulebook:
      buy_pressure + spread کم => جمع‌آوری نرم
      sell_pressure + spread زیاد => عرضه سنگین
      buy_pressure ولی outflow => تله خرید/نیاز به تایید

    Columns (per your MV):
      imbalance5, imbalance_state, spread_pct_avg, net_order_value, ...
    """
    buy_pressure = []
    sell_pressure = []
    spread_wide = []

    # build quick map of sector->net_real_share from intraday_rows (برای accumulation_hint)
    share_map: Dict[str, float] = {}
    for r in intraday_rows or []:
        sec = (r.get("sector") or r.get("sector_key") or r.get("key") or "").strip()
        tv = _f(r.get("total_value"))
        nr = _f(r.get("net_real_value"))
        sh = _safe_div(nr, tv) if (tv is not None and nr is not None) else None
        if sec and sh is not None:
            share_map[sec] = sh

    for r in ob_rows or []:
        sec = (r.get("sector") or "").strip()
        if not sec:
            continue

        imb5 = _f(r.get("imbalance5"))
        st = (r.get("imbalance_state") or "").strip().lower()
        spread = _f(r.get("spread_pct_avg"))

        # state fallback by imbalance5
        if not st:
            if imb5 is not None and imb5 >= 0.20:
                st = "buy_pressure"
            elif imb5 is not None and imb5 <= -0.20:
                st = "sell_pressure"
            else:
                st = "balanced"

        item = {
            "sector": sec,
            "imbalance5": imb5,
            "state": st,
            "spread_pct_avg": spread,
            "buy_concentration": _f(r.get("buy_concentration")),
            "sell_concentration": _f(r.get("sell_concentration")),
        }

        if "buy" in st or st == "buy_pressure":
            buy_pressure.append(item)
        if "sell" in st or st == "sell_pressure":
            sell_pressure.append(item)

        # spread wide threshold (قابل تغییر)
        if spread is not None and spread >= 1.0:
            spread_wide.append(item)

    # sort
    buy_pressure.sort(key=lambda x: float(x.get("imbalance5") or 0), reverse=True)
    sell_pressure.sort(key=lambda x: float(x.get("imbalance5") or 0))
    spread_wide.sort(key=lambda x: float(x.get("spread_pct_avg") or 0), reverse=True)

    # accumulation_hint
    accumulation = []
    trap_buy = []
    soft_accum = []
    heavy_dist = []

    def _is_spread_tight(sp: Optional[float]) -> bool:
        return sp is not None and sp <= 0.5

    for it in buy_pressure[:10]:
        sec = it["sector"]
        sh = share_map.get(sec)
        if sh is None:
            continue

        # buy_pressure + inflow => accumulation candidates
        if sh > 0:
            accumulation.append({"sector": sec, "net_real_share": sh, **it})
            if _is_spread_tight(it.get("spread_pct_avg")):
                soft_accum.append({"sector": sec, "net_real_share": sh, **it})
        else:
            trap_buy.append({"sector": sec, "net_real_share": sh, **it})

    for it in sell_pressure[:10]:
        sec = it["sector"]
        sh = share_map.get(sec)
        if sh is None:
            continue
        if sh < 0 and (it.get("spread_pct_avg") is not None and it["spread_pct_avg"] >= 1.0):
            heavy_dist.append({"sector": sec, "net_real_share": sh, **it})

    return {
        "buy_pressure_sectors": buy_pressure[:5],
        "sell_pressure_sectors": sell_pressure[:5],
        "spread_wide_sectors": spread_wide[:5],
        "accumulation_hint": accumulation[:5],
        "soft_accumulation": soft_accum[:5],
        "trap_buy_hint": trap_buy[:5],
        "heavy_distribution": heavy_dist[:5],
    }


# ----------------------------
# History compare (RS + Baseline)
# ----------------------------

def _history_compare(rs_rows: List[Dict[str, Any]], baseline_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rs_map: Dict[str, Dict[str, Any]] = {}
    for r in rs_rows or []:
        sec = (r.get("sector") or "").strip()
        if not sec:
            continue
        rs_map[sec] = {
            "sector": sec,
            "rs_20d": _f(r.get("rs_20d")),
            "rs_5d": _f(r.get("rs_5d")),
            "rs_60d": _f(r.get("rs_60d")),
        }

    cards: List[Dict[str, Any]] = []
    for b in baseline_rows or []:
        sec = (b.get("sector") or "").strip()
        if not sec:
            continue

        tv = _f(b.get("total_value"))
        nr = _f(b.get("net_real_value"))

        # Z_value
        avg_v20 = _f(b.get("avg_value_20d"))
        std_v20 = _f(b.get("std_value_20d"))
        z_value = None
        if tv is not None and avg_v20 is not None and std_v20 not in (None, 0):
            z_value = (tv - avg_v20) / std_v20

        # Z_real
        avg_r20 = _f(b.get("avg_real_20d"))
        std_r20 = _f(b.get("std_net_real_20d"))
        z_real = None
        if nr is not None and avg_r20 is not None and std_r20 not in (None, 0):
            z_real = (nr - avg_r20) / std_r20

        # net_real_share
        net_real_share = _safe_div(nr, tv)

        rs20 = (rs_map.get(sec) or {}).get("rs_20d")

        card = {
            "sector": sec,
            "rs_20d": rs20,
            "rs_bucket": _bucket_rs20(rs20),
            "net_real_value": nr,
            "total_value": tv,
            "net_real_share": net_real_share,
            "flow_bucket": _bucket_flow_share(net_real_share),
            "z_value": z_value,
            "z_value_bucket": _bucket_z(z_value),
            "z_value_sign": _z_sign(z_value),
            "z_real": z_real,
            "z_real_bucket": _bucket_z(z_real),
            "z_real_sign": _z_sign(z_real),
            "evidence_refs": _ref("daily.sector_rs_latest", "daily.sector_baseline_latest"),
        }
        cards.append(card)

    # leaders/laggards by rs_20d
    cards_rs = [c for c in cards if c.get("rs_20d") is not None]
    rs_sorted = sorted(cards_rs, key=lambda x: float(x.get("rs_20d") or 0), reverse=True)
    rs20_top = rs_sorted[:5]
    rs20_bottom = list(reversed(rs_sorted[-5:])) if len(rs_sorted) >= 5 else list(reversed(rs_sorted))

    # abnormal by z
    zpos_value = sorted([c for c in cards if (c.get("z_value") or 0) > 0], key=lambda x: abs(float(x.get("z_value") or 0)), reverse=True)[:5]
    zneg_value = sorted([c for c in cards if (c.get("z_value") or 0) < 0], key=lambda x: abs(float(x.get("z_value") or 0)), reverse=True)[:5]
    zpos_real = sorted([c for c in cards if (c.get("z_real") or 0) > 0], key=lambda x: abs(float(x.get("z_real") or 0)), reverse=True)[:5]
    zneg_real = sorted([c for c in cards if (c.get("z_real") or 0) < 0], key=lambda x: abs(float(x.get("z_real") or 0)), reverse=True)[:5]

    return {
        "sector_cards": cards,
        "rs20_top": rs20_top,
        "rs20_bottom": rs20_bottom,
        "z_value_pos": zpos_value,
        "z_value_neg": zneg_value,
        "z_real_pos": zpos_real,
        "z_real_neg": zneg_real,
    }


# ----------------------------
# Anomalies (cross-signal)
# ----------------------------

def _build_anomalies(market: Dict[str, Any], orderbook: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    gr = market.get("green_ratio")
    nrv = market.get("net_real_value")

    if gr is not None and nrv is not None:
        if gr > 0.60 and nrv < 0:
            out.append(_anomaly(
                "BREADTH_POS_BUT_REAL_OUTFLOW",
                "پهنای بازار مثبت است اما خروج پول حقیقی دیده می‌شود (ممکن است رشد بدون حمایت پول یا چرخش درون‌گروهی باشد).",
                _ref("signals.market_state.breadth.green_ratio", "signals.market_state.flow.net_real_value"),
                severity="warn",
            ))
        if gr < 0.35 and nrv > 0:
            out.append(_anomaly(
                "BREADTH_NEG_BUT_REAL_INFLOW",
                "پهنای بازار ضعیف است اما ورود پول حقیقی دیده می‌شود (احتمالاً خرید حمایتی روی چند گروه محدود).",
                _ref("signals.market_state.breadth.green_ratio", "signals.market_state.flow.net_real_value"),
                severity="warn",
            ))

    # orderbook trap: buy_pressure but many trap_buy hints
    traps = orderbook.get("trap_buy_hint") or []
    soft = orderbook.get("soft_accumulation") or []
    if traps and not soft:
        out.append(_anomaly(
            "ORDERBOOK_BUY_PRESSURE_BUT_OUTFLOW",
            "فشار خرید در دفتر سفارش‌ها دیده می‌شود اما هم‌زمان نشانه‌های خروج/عدم تأیید پول وجود دارد (احتمال تله خرید؛ نیاز به تأیید).",
            _ref("signals.orderbook.trap_buy_hint"),
            severity="warn",
        ))

    return out[:5]


# ----------------------------
# Public API
# ----------------------------

def build_signals(facts: Dict[str, Any]) -> Dict[str, Any]:
    # ----------------------------
    # ETF extraction (compat)
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
        انتظار: sector شامل ETF prefix باشد.
        """
        buckets: List[Dict[str, Any]] = []
        for r in sector_daily_rows or []:
            s = (r.get("sector") or "").strip()
            if not _is_etf_sector(s):
                continue

            buckets.append({
                "bucket": _etf_bucket_name(s),
                "sector": s,
                "symbols_count": _i(r.get("symbols_count")),
                "total_value": _f(r.get("total_value")),
                "total_volume": _f(r.get("total_volume")),
                "marketcap": _f(r.get("marketcap")),
                "net_real_value": _f(r.get("net_real_value")),
            })

        buckets.sort(key=lambda x: float(x.get("total_value") or 0), reverse=True)
        return buckets

    # ----------------------------
    # Leaders (compat)
    # ----------------------------

    def _leaders_from_daily(
            rs_rows: List[Dict[str, Any]],
            daily_rows: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        خروجی سازگار با نسخه قبلی:
        {
          "rs20_top": [...],
          "rs20_bottom": [...],
          "money_in_top": [...],
          "money_out_top": [...]
        }
        """
        cleaned_rs = []
        for r in rs_rows or []:
            sec = (r.get("sector") or "").strip()
            rs20 = _f(r.get("rs_20d"))
            if not sec or rs20 is None:
                continue
            cleaned_rs.append({
                "sector": sec,
                "rs_20d": rs20,
                "sector_ret_1d": _f(r.get("sector_ret_1d")),
                "market_ret_1d": _f(r.get("market_ret_1d")),
            })

        rs_sorted = sorted(cleaned_rs, key=lambda x: float(x["rs_20d"]), reverse=True)
        rs_top = rs_sorted[:5]
        rs_bottom = list(reversed(rs_sorted[-5:])) if len(rs_sorted) >= 5 else list(reversed(rs_sorted))

        money = []
        for r in daily_rows or []:
            sec = (r.get("sector") or "").strip()
            nrv = _f(r.get("net_real_value"))
            if not sec or nrv is None:
                continue
            money.append({
                "sector": sec,
                "net_real_value": nrv,
                "total_value": _f(r.get("total_value")),
            })

        money_in = sorted(
            [m for m in money if (m.get("net_real_value") or 0) > 0],
            key=lambda x: float(x.get("net_real_value") or 0),
            reverse=True
        )[:5]

        money_out = sorted(
            [m for m in money if (m.get("net_real_value") or 0) < 0],
            key=lambda x: float(x.get("net_real_value") or 0)
        )[:5]

        return {
            "rs20_top": rs_top,
            "rs20_bottom": rs_bottom,
            "money_in_top": money_in,
            "money_out_top": money_out,
        }

    def _clip01(x: float) -> float:

        if x < 0:
            return 0.0
        if x > 1:
            return 1.0
        return float(x)

    def _classify_market_state(
            *,
            intraday_green: Optional[float],
            intraday_eqw: Optional[float],
            intraday_net_real: Optional[int],
            intraday_imb5: Optional[float],
            intraday_imb_state: Optional[str],
            leaders: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        ✅ Backward-compatible wrapper
        - اگر کد قدیمی هنوز _classify_market_state را صدا زد، اینجا جواب می‌گیرد.
        - خروجی همان ساختار old-style است:
          market_state + anomalies
        """

        # --- scoring ساده مثل نسخه قبلی
        gr = intraday_green
        eqw = intraday_eqw
        nrv = intraday_net_real
        imb5 = intraday_imb5
        st = (intraday_imb_state or "").lower() if intraday_imb_state else ""

        score = 0.0
        w = 0.0

        if gr is not None:
            if gr < 0.35:
                score -= 1.0
            elif gr > 0.60:
                score += 1.0
            w += 1.0

        if nrv is not None:
            score += 1.0 if nrv > 0 else (-1.0 if nrv < 0 else 0.0)
            w += 1.0

        # orderbook contribution
        if st:
            if ("buy" in st) or ("bull" in st):
                score += 0.7
                w += 0.7
            elif ("sell" in st) or ("bear" in st):
                score -= 0.7
                w += 0.7
            else:
                w += 0.3
        elif imb5 is not None:
            if imb5 > 0.15:
                score += 0.7
            elif imb5 < -0.15:
                score -= 0.7
            w += 0.7

        if w == 0:
            market_state = {
                "breadth": {"green_ratio": None, "eqw_avg_ret_pct": None},
                "flow": {"net_real_value": None},
                "orderbook": {"imbalance5": None, "state": None},
                "trend": "mixed",
                "regime": "neutral",
                "confidence": 0.4,
            }
            return market_state, []

        norm_score = score / w

        if norm_score > 0.25:
            trend = "bullish"
        elif norm_score < -0.25:
            trend = "bearish"
        else:
            trend = "mixed"

        if gr is not None and nrv is not None:
            if gr > 0.55 and nrv > 0:
                regime = "risk_on"
            elif gr < 0.40 and nrv < 0:
                regime = "risk_off"
            else:
                regime = "neutral"
        else:
            regime = "neutral"

        available = sum(1 for v in [gr, nrv, intraday_imb_state or imb5] if v is not None)
        base_conf = 0.45 + 0.15 * available
        align_boost = min(0.10, 0.10 * abs(norm_score))
        confidence = _clip01(base_conf + align_boost)

        market_state = {
            "breadth": {"green_ratio": gr, "eqw_avg_ret_pct": eqw},
            "flow": {"net_real_value": nrv},
            "orderbook": {"imbalance5": imb5, "state": intraday_imb_state},
            "trend": trend,
            "regime": regime,
            "confidence": confidence,
        }

        # anomalies مثل قبل
        anomalies: List[Dict[str, Any]] = []
        if gr is not None and nrv is not None:
            if gr > 0.60 and nrv < 0:
                anomalies.append(_anomaly(
                    "BREADTH_POS_BUT_REAL_OUTFLOW",
                    "پهنای بازار مثبت است اما خروج پول حقیقی دیده می‌شود (ممکن است رشد بدون حمایت پول یا چرخش درون‌گروهی باشد).",
                    _ref("intraday.market_snapshot.green_ratio", "intraday.market_snapshot.net_real_value"),
                    severity="warn",
                ))
            if gr < 0.35 and nrv > 0:
                anomalies.append(_anomaly(
                    "BREADTH_NEG_BUT_REAL_INFLOW",
                    "پهنای بازار ضعیف است اما ورود پول حقیقی دیده می‌شود (احتمالاً خرید حمایتی روی چند گروه محدود).",
                    _ref("intraday.market_snapshot.green_ratio", "intraday.market_snapshot.net_real_value"),
                    severity="warn",
                ))

        return market_state, anomalies

    daily = (facts or {}).get("daily") or {}
    intraday = (facts or {}).get("intraday") or {}

    # --------------------------------------------------
    # 1️⃣ Intraday snapshot (همان نسخه قبلی تو)
    # --------------------------------------------------

    mshot = intraday.get("market_snapshot") or {}

    intraday_green = _norm_green_ratio(_to_float(mshot.get("green_ratio")))
    intraday_eqw = _to_float(mshot.get("eqw_avg_ret_pct"))
    intraday_total_value = _to_int(mshot.get("total_value"))
    intraday_net_real = _to_int(mshot.get("net_real_value"))
    intraday_net_legal = _to_int(mshot.get("net_legal_value"))
    intraday_imb5 = _to_float(mshot.get("imbalance5"))
    intraday_imb_state = (
        mshot.get("imbalance_state")
        or mshot.get("imbalanceState")
        or mshot.get("IMBALANCE_STATE")
    )

    # --------------------------------------------------
    # 2️⃣ ETF (نسخه قبلی تو)
    # --------------------------------------------------

    sec_daily_rows = daily.get("sector_daily_latest") or []
    etf_buckets = _extract_etf_buckets(sec_daily_rows)

    # --------------------------------------------------
    # 3️⃣ Leaders (نسخه قبلی تو)
    # --------------------------------------------------

    sec_rs_rows = daily.get("sector_rs_latest") or []
    leaders = _leaders_from_daily(sec_rs_rows, sec_daily_rows)

    # --------------------------------------------------
    # 4️⃣ Market classification (نسخه قبلی تو)
    # --------------------------------------------------

    market_state, anomalies_old = _classify_market_state(
        intraday_green=intraday_green,
        intraday_eqw=intraday_eqw,
        intraday_net_real=intraday_net_real,
        intraday_imb5=intraday_imb5,
        intraday_imb_state=intraday_imb_state,
        leaders=leaders,
    )

    # --------------------------------------------------
    # 5️⃣ Intraday active sectors (جدید)
    # --------------------------------------------------

    intraday_rows = (
        intraday.get("sector_rows_at_ts")
        or intraday.get("sector_snapshots")
        or []
    )

    active = _active_sectors_from_intraday_rows(intraday_rows)

    # --------------------------------------------------
    # 6️⃣ Orderbook advanced rules (جدید)
    # --------------------------------------------------

    ob_rows = (intraday.get("mv_orderbook_report") or {}).get("rows") or []
    orderbook = _orderbook_signals(ob_rows, intraday_rows)

    # --------------------------------------------------
    # 7️⃣ History compare (RS + Z-score) جدید
    # --------------------------------------------------

    baseline_rows = daily.get("sector_baseline_latest") or []
    history = _history_compare(sec_rs_rows, baseline_rows)

    # --------------------------------------------------
    # 8️⃣ Anomalies (merge قدیم + جدید)
    # --------------------------------------------------

    anomalies_new = _build_anomalies(
        {
            "green_ratio": intraday_green,
            "net_real_value": intraday_net_real,
        },
        orderbook,
    )

    anomalies = (anomalies_old or []) + (anomalies_new or [])
    anomalies = anomalies[:6]

    # --------------------------------------------------
    # 9️⃣ ETF wrapper
    # --------------------------------------------------

    etf_signals = {
        "available": bool(etf_buckets),
        "buckets": etf_buckets,
    }

    # --------------------------------------------------
    # 🔟 LLM capsule (قابل توسعه)
    # --------------------------------------------------

    llm_capsule = {
        "market_state": market_state,
        "leaders": leaders,
        "active_sectors": active,
        "orderbook": orderbook,
        "history_compare": {
            "rs20_top": history.get("rs20_top"),
            "rs20_bottom": history.get("rs20_bottom"),
            "z_value_pos": history.get("z_value_pos"),
            "z_value_neg": history.get("z_value_neg"),
            "z_real_pos": history.get("z_real_pos"),
            "z_real_neg": history.get("z_real_neg"),
        },
        "etf": etf_signals,
        "anomalies": anomalies,
    }

    # --------------------------------------------------
    # Final return
    # --------------------------------------------------

    return {
        "market_state": market_state,
        "leaders": leaders,
        "active_sectors": active,
        "orderbook": orderbook,
        "history_compare": history,
        "etf": etf_signals,
        "anomalies": anomalies,
        "llm_capsule": llm_capsule,
    }