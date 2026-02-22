# backend/commentary/narrative.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Literal, Tuple
from datetime import datetime


Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]


# ----------------------------
# Helpers: formatting
# ----------------------------

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None

def _pct(x: Optional[float], digits: int = 2) -> Optional[str]:
    if x is None:
        return None
    try:
        return f"{x:.{digits}f}%"
    except Exception:
        return None

def _fmt_num(x: Optional[float], digits: int = 0) -> Optional[str]:
    if x is None:
        return None
    try:
        if digits == 0:
            return f"{int(round(float(x))):,}"
        return f"{float(x):,.{digits}f}"
    except Exception:
        return None

def _fmt_money_rial(x: Optional[int]) -> Optional[str]:
    """
    نمایش ریالی خوانا. (مثلا 12,345,678,900,000)
    اگر دوست داشتی می‌تونی بعداً تبدیل به میلیارد/همت هم بدی.
    """
    if x is None:
        return None
    return f"{x:,} ریال"

def _sign_label(x: Optional[float]) -> Optional[str]:
    if x is None:
        return None
    if x > 0:
        return "مثبت"
    if x < 0:
        return "منفی"
    return "خنثی"

def _join_top(items: List[Dict[str, Any]], key_name: str = "sector", n: int = 3) -> str:
    xs = []
    for it in (items or [])[:n]:
        v = it.get(key_name) or it.get("name") or it.get("key")
        if v:
            xs.append(str(v))
    return "، ".join(xs)

def _evidence(*paths: str) -> List[str]:
    return [p for p in paths if p]


# ----------------------------
# Narrative item schema (simple dict)
# ----------------------------

def _item(
    text: str,
    evidence_refs: Optional[List[str]] = None,
    confidence: float = 0.7,
    tags: Optional[List[str]] = None,
    severity: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "text": text,
        "evidence_refs": evidence_refs or [],
        "confidence": float(confidence),
        "tags": tags or [],
        "severity": severity,
    }


# ----------------------------
# Core: public/pro generation
# ----------------------------

def build_narrative(
    *,
    mode: Mode,
    audience: Audience,
    meta: Dict[str, Any],
    facts: Dict[str, Any],
    signals: Dict[str, Any],
) -> Dict[str, Any]:
    """
    خروجی:
      {
        "headline": [ {text,evidence_refs,confidence,tags,...}, ... ],
        "bullets":  [ ... ],
        "paragraphs": [ ... ]
      }

    ورودی‌ها:
      meta: { asof: {daily_date, intraday_ts, ...}, ... }
      facts: { daily: {...}, intraday: {...} }
      signals: { market_state: {...}, leaders: {...}, etf: {...}, anomalies:[...] }
    """

    ms = (signals or {}).get("market_state", {}) or {}
    leaders = (signals or {}).get("leaders", {}) or {}
    etf = (signals or {}).get("etf", {}) or {}
    anomalies = (signals or {}).get("anomalies", []) or []

    daily_date = ((meta or {}).get("asof", {}) or {}).get("daily_date")
    intraday_ts = ((meta or {}).get("asof", {}) or {}).get("intraday_ts")

    # Extract common metrics (intraday-first, then daily if needed)
    breadth = (ms.get("breadth") or {})
    flow = (ms.get("flow") or {})
    orderbook = (ms.get("orderbook") or {})

    green_ratio = _safe_float(breadth.get("green_ratio"))
    eqw_ret = _safe_float(ms.get("eqw_avg_ret_pct") or breadth.get("eqw_avg_ret_pct"))
    # NOTE: خیلی از جاها eqw_avg_ret_pct داخل facts.intraday.market میاد،
    # ولی ما ترجیح می‌دیم signals.market_state از قبل پرش کنه. اگر نکرد، در signals.py اضافه کن.

    net_real_value = _safe_int(flow.get("net_real_value"))
    imbalance5 = _safe_float(orderbook.get("imbalance5"))
    imbalance_state = orderbook.get("state")

    regime = ms.get("regime")  # risk_on / risk_off / neutral
    trend = ms.get("trend")    # bullish / bearish / mixed
    conf = float(ms.get("confidence", 0.65))

    rs_top = leaders.get("rs20_top") or []
    rs_bottom = leaders.get("rs20_bottom") or []
    money_in_top = leaders.get("money_in_top") or []
    money_out_top = leaders.get("money_out_top") or []

    # ETF buckets summary (expected from your mv_sector_daily_latest after you fixed it)
    etf_available = bool(etf.get("available", False))
    etf_buckets = etf.get("buckets") or []  # list of {bucket, total_value, net_real_value, rs_20d? ...}

    # ----------------------------
    # Build layered content
    # ----------------------------

    headline_items: List[Dict[str, Any]] = []
    bullet_items: List[Dict[str, Any]] = []
    paragraph_items: List[Dict[str, Any]] = []

    # 1) HEADLINES
    if mode == "public":
        headline_items.extend(_public_headlines(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            regime=regime,
            trend=trend,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            conf=conf,
            anomalies=anomalies,
        ))
    else:
        headline_items.extend(_pro_headlines(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            regime=regime,
            trend=trend,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            imbalance_state=imbalance_state,
            conf=conf,
            anomalies=anomalies,
        ))

    # 2) BULLETS
    if mode == "public":
        bullet_items.extend(_public_bullets(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            imbalance5=imbalance5,
            imbalance_state=imbalance_state,
            rs_top=rs_top,
            rs_bottom=rs_bottom,
            etf_available=etf_available,
            etf_buckets=etf_buckets,
            conf=conf,
            anomalies=anomalies,
        ))
    else:
        bullet_items.extend(_pro_bullets(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            imbalance5=imbalance5,
            imbalance_state=imbalance_state,
            rs_top=rs_top,
            rs_bottom=rs_bottom,
            money_in_top=money_in_top,
            money_out_top=money_out_top,
            etf_available=etf_available,
            etf_buckets=etf_buckets,
            conf=conf,
            anomalies=anomalies,
        ))

    # 3) PARAGRAPHS (Daily + Intraday narrative composition)
    if mode == "public":
        paragraph_items.extend(_public_paragraphs(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            regime=regime,
            trend=trend,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            imbalance5=imbalance5,
            imbalance_state=imbalance_state,
            rs_top=rs_top,
            rs_bottom=rs_bottom,
            etf_available=etf_available,
            etf_buckets=etf_buckets,
            conf=conf,
            anomalies=anomalies,
        ))
    else:
        paragraph_items.extend(_pro_paragraphs(
            daily_date=daily_date,
            intraday_ts=intraday_ts,
            regime=regime,
            trend=trend,
            green_ratio=green_ratio,
            net_real_value=net_real_value,
            imbalance5=imbalance5,
            imbalance_state=imbalance_state,
            rs_top=rs_top,
            rs_bottom=rs_bottom,
            money_in_top=money_in_top,
            money_out_top=money_out_top,
            etf_available=etf_available,
            etf_buckets=etf_buckets,
            conf=conf,
            anomalies=anomalies,
        ))

    # Audience filter
    out = {
        "headline": headline_items,
        "bullets": bullet_items,
        "paragraphs": paragraph_items,
    }

    if audience == "headline":
        return {"headline": out["headline"], "bullets": [], "paragraphs": []}
    if audience == "bullets":
        return {"headline": out["headline"], "bullets": out["bullets"], "paragraphs": []}
    if audience == "paragraphs":
        return {"headline": out["headline"], "bullets": out["bullets"], "paragraphs": out["paragraphs"]}
    return out


# ----------------------------
# Templates: PUBLIC
# ----------------------------

def _public_headlines(
    *,
    daily_date: Any,
    intraday_ts: Any,
    regime: Optional[str],
    trend: Optional[str],
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    parts = []

    # primary label
    if trend == "bearish" or (green_ratio is not None and green_ratio < 0.35) or (net_real_value is not None and net_real_value < 0):
        t = "بازار امروز بیشتر متمایل به فروش بود."
    elif trend == "bullish" or (green_ratio is not None and green_ratio > 0.60) or (net_real_value is not None and net_real_value > 0):
        t = "بازار امروز بهتر از حالت عادی بود و تقاضا قوی‌تر دیده شد."
    else:
        t = "بازار امروز متعادل و نوسانی بود."
    parts.append(_item(
        t,
        evidence_refs=_evidence("signals.market_state"),
        confidence=min(0.9, max(0.55, conf)),
        tags=["market_state", "headline"],
    ))

    # add a second headline if conflict exists
    if anomalies:
        parts.append(_item(
            "برخی سیگنال‌ها هم‌جهت نیستند و ممکن است نیاز به احتیاط در برداشت داشته باشد.",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.65,
            tags=["anomaly", "headline"],
            severity="warn",
        ))

    return parts[:2]


def _public_bullets(
    *,
    daily_date: Any,
    intraday_ts: Any,
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    imbalance5: Optional[float],
    imbalance_state: Optional[str],
    rs_top: List[Dict[str, Any]],
    rs_bottom: List[Dict[str, Any]],
    etf_available: bool,
    etf_buckets: List[Dict[str, Any]],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if green_ratio is not None:
        out.append(_item(
            f"سهم نمادهای مثبت حدود { _pct(green_ratio*100 if green_ratio<=1 else green_ratio, 1) or '' } بود.",
            evidence_refs=_evidence("signals.market_state.breadth.green_ratio"),
            confidence=0.78,
            tags=["breadth", "intraday"],
        ))

    if net_real_value is not None:
        direction = "ورود" if net_real_value > 0 else "خروج" if net_real_value < 0 else "خنثی"
        out.append(_item(
            f"{direction} پول حقیقی در بازار ثبت شد.",
            evidence_refs=_evidence("signals.market_state.flow.net_real_value"),
            confidence=0.78,
            tags=["flow", "intraday"],
        ))

    if imbalance_state:
        out.append(_item(
            f"در دفتر سفارش‌ها وضعیت کلی «{imbalance_state}» گزارش شده است.",
            evidence_refs=_evidence("signals.market_state.orderbook.state"),
            confidence=0.70,
            tags=["orderbook", "intraday"],
        ))

    # RS leaders
    top_s = _join_top(rs_top, "sector", 3)
    bot_s = _join_top(rs_bottom, "sector", 3)
    if top_s:
        out.append(_item(
            f"در نگاه ۲۰ روزه، گروه‌های قوی‌تر: {top_s}.",
            evidence_refs=_evidence("signals.leaders.rs20_top"),
            confidence=0.72,
            tags=["rs", "daily"],
        ))
    if bot_s:
        out.append(_item(
            f"گروه‌های ضعیف‌تر: {bot_s}.",
            evidence_refs=_evidence("signals.leaders.rs20_bottom"),
            confidence=0.72,
            tags=["rs", "daily"],
        ))

    # ETF summary (public - minimal)
    if etf_available and etf_buckets:
        # pick 2 buckets by total_value
        sorted_b = sorted(etf_buckets, key=lambda x: float(x.get("total_value") or 0), reverse=True)
        top_buckets = [b.get("bucket") for b in sorted_b[:2] if b.get("bucket")]
        if top_buckets:
            out.append(_item(
                f"در صندوق‌های قابل معامله، توجه بیشتر روی: { '، '.join(top_buckets) }.",
                evidence_refs=_evidence("signals.etf.buckets"),
                confidence=0.68,
                tags=["etf", "daily"],
            ))

    if anomalies:
        out.append(_item(
            "توجه: برخی شاخص‌ها با هم ناسازگارند (ممکن است به زمان ثبت داده‌ها مربوط باشد).",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.62,
            tags=["anomaly"],
            severity="warn",
        ))

    # keep bullets short
    return out[:7]


def _public_paragraphs(
    *,
    daily_date: Any,
    intraday_ts: Any,
    regime: Optional[str],
    trend: Optional[str],
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    imbalance5: Optional[float],
    imbalance_state: Optional[str],
    rs_top: List[Dict[str, Any]],
    rs_bottom: List[Dict[str, Any]],
    etf_available: bool,
    etf_buckets: List[Dict[str, Any]],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # Paragraph 1: Intraday state
    p1 = []
    if intraday_ts:
        p1.append("در آخرین وضعیت لحظه‌ای ثبت‌شده")
    else:
        p1.append("در وضعیت فعلی بازار")

    if green_ratio is not None:
        p1.append(f"پهنای بازار ضعیف/قوی نبود و سهم مثبت‌ها حدود {_pct(green_ratio*100 if green_ratio<=1 else green_ratio, 1)} بود")
    if net_real_value is not None:
        if net_real_value < 0:
            p1.append("هم‌زمان خروج پول حقیقی دیده شد")
        elif net_real_value > 0:
            p1.append("هم‌زمان ورود پول حقیقی دیده شد")
        else:
            p1.append("و جریان پول حقیقی خنثی بود")

    if imbalance_state:
        p1.append(f"و در دفتر سفارش‌ها نشانه‌ی «{imbalance_state}» گزارش شده است")

    text1 = "، ".join([x for x in p1 if x]) + "."
    out.append(_item(
        text1,
        evidence_refs=_evidence("signals.market_state"),
        confidence=min(0.88, max(0.55, conf)),
        tags=["paragraph", "intraday"],
    ))

    # Paragraph 2: Daily context + RS
    top_s = _join_top(rs_top, "sector", 3)
    bot_s = _join_top(rs_bottom, "sector", 3)

    p2 = []
    if daily_date:
        p2.append("در جمع‌بندی روزانه")
    else:
        p2.append("در جمع‌بندی")

    if top_s:
        p2.append(f"گروه‌های قوی‌تر (نسبت به بازار) شامل {top_s} بودند")
    if bot_s:
        p2.append(f"و گروه‌های ضعیف‌تر شامل {bot_s}")

    if etf_available and etf_buckets:
        # mention 1-2 interesting ETF buckets by net_real_value magnitude
        sorted_flow = sorted(etf_buckets, key=lambda x: abs(float(x.get("net_real_value") or 0)), reverse=True)
        b = sorted_flow[0] if sorted_flow else None
        if b and b.get("bucket"):
            p2.append(f"در صندوق‌های قابل معامله نیز، گروه «{b['bucket']}» از نظر جریان پول قابل توجه بوده است")

    text2 = "، ".join([x for x in p2 if x]) + "."
    out.append(_item(
        text2,
        evidence_refs=_evidence("signals.leaders", "signals.etf"),
        confidence=0.70,
        tags=["paragraph", "daily"],
    ))

    # Paragraph 3: conflict note
    if anomalies:
        out.append(_item(
            "نکته: اگر سیگنال‌ها هم‌جهت نبودند، بهتر است با احتیاط تصمیم‌گیری شود و چند نوبت داده‌ی بعدی هم بررسی شود.",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.62,
            tags=["paragraph", "anomaly"],
            severity="warn",
        ))

    return out[:3]


# ----------------------------
# Templates: PRO
# ----------------------------

def _pro_headlines(
    *,
    daily_date: Any,
    intraday_ts: Any,
    regime: Optional[str],
    trend: Optional[str],
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    imbalance_state: Optional[str],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # main pro headline
    regime_txt = "Risk-off" if regime == "risk_off" else "Risk-on" if regime == "risk_on" else "Neutral"
    breadth_txt = ""
    if green_ratio is not None:
        breadth_txt = f"breadth={_fmt_num(green_ratio, 3)}"
    flow_txt = ""
    if net_real_value is not None:
        flow_txt = f"real_flow={_fmt_money_rial(net_real_value)}"
    ob_txt = f"orderbook={imbalance_state}" if imbalance_state else ""

    pieces = [p for p in [regime_txt, trend, breadth_txt, flow_txt, ob_txt] if p]
    out.append(_item(
        " | ".join(pieces) if pieces else "Market snapshot آماده نیست.",
        evidence_refs=_evidence("signals.market_state"),
        confidence=min(0.92, max(0.55, conf)),
        tags=["market_state", "headline", "pro"],
    ))

    if anomalies:
        out.append(_item(
            "Signal divergence: برخی شاخص‌ها با هم هم‌جهت نیستند (نیاز به تأیید).",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.70,
            tags=["anomaly", "headline", "pro"],
            severity="warn",
        ))

    return out[:2]


def _pro_bullets(
    *,
    daily_date: Any,
    intraday_ts: Any,
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    imbalance5: Optional[float],
    imbalance_state: Optional[str],
    rs_top: List[Dict[str, Any]],
    rs_bottom: List[Dict[str, Any]],
    money_in_top: List[Dict[str, Any]],
    money_out_top: List[Dict[str, Any]],
    etf_available: bool,
    etf_buckets: List[Dict[str, Any]],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    if green_ratio is not None:
        out.append(_item(
            f"Breadth (green_ratio): {_fmt_num(green_ratio, 4)}",
            evidence_refs=_evidence("signals.market_state.breadth.green_ratio"),
            confidence=0.80,
            tags=["intraday", "breadth", "pro"],
        ))

    if net_real_value is not None:
        out.append(_item(
            f"Real money flow: {_fmt_money_rial(net_real_value)} ({'inflow' if net_real_value>0 else 'outflow' if net_real_value<0 else 'flat'})",
            evidence_refs=_evidence("signals.market_state.flow.net_real_value"),
            confidence=0.80,
            tags=["intraday", "flow", "pro"],
        ))

    if imbalance_state:
        out.append(_item(
            f"Orderbook state: {imbalance_state}" + (f" | imbalance5={_fmt_num(imbalance5, 4)}" if imbalance5 is not None else ""),
            evidence_refs=_evidence("signals.market_state.orderbook"),
            confidence=0.72,
            tags=["intraday", "orderbook", "pro"],
        ))

    # RS leaders/laggards
    top_s = _join_top(rs_top, "sector", 5)
    bot_s = _join_top(rs_bottom, "sector", 5)
    if top_s:
        out.append(_item(
            f"RS20 leaders: {top_s}",
            evidence_refs=_evidence("signals.leaders.rs20_top"),
            confidence=0.75,
            tags=["daily", "rs", "pro"],
        ))
    if bot_s:
        out.append(_item(
            f"RS20 laggards: {bot_s}",
            evidence_refs=_evidence("signals.leaders.rs20_bottom"),
            confidence=0.75,
            tags=["daily", "rs", "pro"],
        ))

    # Money leaders
    mi = _join_top(money_in_top, "sector", 3)
    mo = _join_top(money_out_top, "sector", 3)
    if mi:
        out.append(_item(
            f"Top real inflow sectors (daily): {mi}",
            evidence_refs=_evidence("signals.leaders.money_in_top"),
            confidence=0.70,
            tags=["daily", "flow", "pro"],
        ))
    if mo:
        out.append(_item(
            f"Top real outflow sectors (daily): {mo}",
            evidence_refs=_evidence("signals.leaders.money_out_top"),
            confidence=0.70,
            tags=["daily", "flow", "pro"],
        ))

    # ETF buckets highlight
    if etf_available and etf_buckets:
        # show top 3 ETF buckets by total_value
        sorted_b = sorted(etf_buckets, key=lambda x: float(x.get("total_value") or 0), reverse=True)
        bnames = [b.get("bucket") for b in sorted_b[:3] if b.get("bucket")]
        if bnames:
            out.append(_item(
                f"ETF buckets (top by value): {', '.join(bnames)}",
                evidence_refs=_evidence("signals.etf.buckets"),
                confidence=0.70,
                tags=["daily", "etf", "pro"],
            ))

    if anomalies:
        out.append(_item(
            "Divergence detected between microstructure and price/breadth or flows.",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.68,
            tags=["anomaly", "pro"],
            severity="warn",
        ))

    return out[:9]


def _pro_paragraphs(
    *,
    daily_date: Any,
    intraday_ts: Any,
    regime: Optional[str],
    trend: Optional[str],
    green_ratio: Optional[float],
    net_real_value: Optional[int],
    imbalance5: Optional[float],
    imbalance_state: Optional[str],
    rs_top: List[Dict[str, Any]],
    rs_bottom: List[Dict[str, Any]],
    money_in_top: List[Dict[str, Any]],
    money_out_top: List[Dict[str, Any]],
    etf_available: bool,
    etf_buckets: List[Dict[str, Any]],
    conf: float,
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # P1: intraday regime
    p1 = []
    if intraday_ts:
        p1.append(f"Intraday snapshot ({intraday_ts}) نشان می‌دهد")
    else:
        p1.append("Intraday snapshot نشان می‌دهد")

    if green_ratio is not None:
        p1.append(f"breadth با green_ratio={_fmt_num(green_ratio, 4)} در سطح {('ضعیف' if green_ratio < 0.35 else 'متوسط' if green_ratio < 0.60 else 'قوی')} است")
    if net_real_value is not None:
        p1.append(f"و real money flow برابر {_fmt_money_rial(net_real_value)} است")
    if imbalance_state:
        p1.append(f"(orderbook={imbalance_state}" + (f", imbalance5={_fmt_num(imbalance5,4)})" if imbalance5 is not None else ")"))

    out.append(_item(
        "؛ ".join([x for x in p1 if x]) + ".",
        evidence_refs=_evidence("signals.market_state"),
        confidence=min(0.90, max(0.55, conf)),
        tags=["paragraph", "intraday", "pro"],
    ))

    # P2: daily structure + RS + flow leaders
    top_s = _join_top(rs_top, "sector", 4)
    bot_s = _join_top(rs_bottom, "sector", 4)
    mi = _join_top(money_in_top, "sector", 3)
    mo = _join_top(money_out_top, "sector", 3)

    p2 = []
    if daily_date:
        p2.append(f"در نمای روزانه ({daily_date})")
    else:
        p2.append("در نمای روزانه")

    if top_s:
        p2.append(f"RS20 leaders شامل {top_s} هستند")
    if bot_s:
        p2.append(f"و laggards شامل {bot_s}")
    if mi:
        p2.append(f"از منظر جریان پول حقیقی، بیشترین inflow در {mi} دیده می‌شود")
    if mo:
        p2.append(f"و بیشترین outflow در {mo}")

    out.append(_item(
        "؛ ".join([x for x in p2 if x]) + ".",
        evidence_refs=_evidence("signals.leaders"),
        confidence=0.74,
        tags=["paragraph", "daily", "pro"],
    ))

    # P3: ETF note + divergence note
    p3 = []
    if etf_available and etf_buckets:
        sorted_b = sorted(etf_buckets, key=lambda x: float(x.get("total_value") or 0), reverse=True)
        bnames = [b.get("bucket") for b in sorted_b[:4] if b.get("bucket")]
        if bnames:
            p3.append("ETF segmentation فعال است و bucketهای پرتراکنش شامل " + "، ".join(bnames) + " هستند")
    if anomalies:
        p3.append("هم‌چنین divergence بین برخی سیگنال‌ها دیده می‌شود (احتمال اختلاف زمان برداشت داده/تفاوت بین سفارش و معامله).")

    if p3:
        out.append(_item(
            " ".join(p3),
            evidence_refs=_evidence("signals.etf", "signals.anomalies"),
            confidence=0.68,
            tags=["paragraph", "etf", "anomaly", "pro"],
            severity="warn" if anomalies else None,
        ))

    return out[:3]