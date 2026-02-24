# backend/commentary/narrative.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Literal


Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]


# ----------------------------
# Helpers: safe casts / formatting
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
    if x is None:
        return None
    return f"{x:,} ریال"


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
# Landing guest sections order
# ----------------------------

SECTION_ORDER_GUEST = [
    ("header", "خلاصه لحظه‌ای"),
    ("market_overview", "وضعیت کلی بازار"),
    ("active_sectors", "صنایع فعال"),
    ("orderbook", "جریان سفارشات"),
    ("anomalies", "هشدارهای مهم"),
    ("real_legal", "رفتار حقیقی/حقوقی"),
    ("history_compare", "مقایسه با گذشته"),
    ("morning_story", "روایت صبح تا الان"),
]


# ----------------------------
# Enforcement helpers (standards)
# ----------------------------

_SENT_SPLIT_RE = re.compile(r'(?<=[\.\!\؟\?])\s+')


def _cap_lines(text: str, max_lines: int = 2) -> str:
    if not text:
        return ""
    lines = [ln.strip() for ln in str(text).splitlines() if ln.strip()]
    return "\n".join(lines[:max_lines]).strip()


def _cap_sentences(text: str, max_sentences: int = 2) -> str:
    if not text:
        return ""
    s = " ".join(str(text).strip().split())
    parts = [p.strip() for p in _SENT_SPLIT_RE.split(s) if p.strip()]
    if not parts:
        return s
    return " ".join(parts[:max_sentences]).strip()


def _ensure_bullet_bounds(bullets: List[Dict[str, Any]], max_n: int = 4) -> List[Dict[str, Any]]:
    b = bullets or []
    if len(b) > max_n:
        b = b[:max_n]
    return b


def _fallback(section_id: str) -> str:
    mapping = {
        "header": "خلاصه لحظه‌ای آماده نیست.",
        "market_overview": "اطلاعات کافی برای جمع‌بندی وضعیت کلی بازار موجود نیست.",
        "active_sectors": "اطلاعات کافی برای تعیین صنایع فعال موجود نیست.",
        "orderbook": "اطلاعات کافی برای تحلیل جریان سفارشات موجود نیست.",
        "anomalies": "هشدار خاصی در این لحظه ثبت نشده است.",
        "real_legal": "اطلاعات کافی برای جمع‌بندی رفتار حقیقی/حقوقی موجود نیست.",
        "history_compare": "اطلاعات کافی برای مقایسه با گذشته موجود نیست.",
        "morning_story": "اطلاعات کافی برای روایت زمانی صبح تا الان موجود نیست.",
    }
    return mapping.get(section_id, "اطلاعات کافی نیست.")


def _enforce_section(
    section: Dict[str, Any],
    *,
    section_id: str,
    is_header: bool,
    max_bullets: int,
) -> Dict[str, Any]:
    section["id"] = section.get("id") or section_id
    section["title"] = section.get("title") or ""
    section["text"] = section.get("text") or ""
    section["bullets"] = section.get("bullets") or []
    section["locks"] = section.get("locks") or []
    # section["cta"] can be None or dict

    if is_header:
        section["text"] = _cap_lines(section["text"], max_lines=2)
    else:
        section["text"] = _cap_sentences(section["text"], max_sentences=2)

    if not section["text"].strip():
        section["text"] = _fallback(section_id)

    section["bullets"] = _ensure_bullet_bounds(section["bullets"], max_n=max_bullets)

    return section


# ----------------------------
# Pick helpers for bullets/anomalies from old lists
# ----------------------------

def _first_text(items: List[Dict[str, Any]]) -> str:
    for it in items or []:
        t = (it or {}).get("text")
        if t:
            return str(t)
    return ""


def _pick_by_tag(items: List[Dict[str, Any]], any_tags: List[str], max_n: int) -> List[Dict[str, Any]]:
    out = []
    tags_set = set(any_tags or [])
    for it in items or []:
        itags = set((it or {}).get("tags") or [])
        if itags & tags_set:
            out.append(it)
        if len(out) >= max_n:
            break
    return out


def _anomaly_items(anoms: List[Dict[str, Any]], max_n: int = 3) -> List[Dict[str, Any]]:
    out = []
    for a in (anoms or [])[:max_n]:
        txt = a.get("text") or a.get("message") or a.get("code")
        if not txt:
            continue
        out.append(_item(
            str(txt),
            evidence_refs=a.get("evidence_refs") or _evidence("signals.anomalies"),
            confidence=0.65 if a.get("severity") == "warn" else 0.70,
            tags=["anomaly"],
            severity=a.get("severity") or "warn",
        ))
    return out


# ----------------------------
# CTA consolidation
# ----------------------------

def _cta_upgrade(message: str, target: str = "pro") -> Dict[str, Any]:
    return {
        "placement": "after_section",
        "message": message,
        "action": "upgrade",
        "target": target,
    }


def _public_cta_after_anomalies() -> Dict[str, Any]:
    return _cta_upgrade(
        "می‌خواهید دقیقاً بدانید پول امروز «به کدام نمادها» رفته و چرخش بین صنایع در چه بازه‌ای اتفاق افتاده؟ نسخه حرفه‌ای این جزئیات را نشان می‌دهد.",
        target="pro",
    )


def _public_cta_end_of_page() -> Dict[str, Any]:
    return _cta_upgrade(
        "نسخه حرفه‌ای: گزارش صنعت‌به‌صنعت + سهم‌به‌سهم، Z-score غیرعادی‌ها، و روایت دقیق فازهای صبح تا الان.",
        target="pro",
    )


# ----------------------------
# Build sections (new narrative output)
# ----------------------------

def _build_sections(
    *,
    mode: Mode,
    headline_items: List[Dict[str, Any]],
    bullet_items: List[Dict[str, Any]],
    paragraph_items: List[Dict[str, Any]],
    signals: Dict[str, Any],
) -> List[Dict[str, Any]]:
    ms = (signals or {}).get("market_state", {}) or {}
    leaders = (signals or {}).get("leaders", {}) or {}
    anomalies = (signals or {}).get("anomalies", []) or []

    # --- SECTION payloads (raw)
    sec_map: Dict[str, Dict[str, Any]] = {}

    # 1) Header (2 lines)
    header_lines = [it.get("text") for it in (headline_items or [])[:2] if it.get("text")]
    header_text = "\n".join(header_lines).strip()
    sec_map["header"] = {"text": header_text, "bullets": [], "locks": [], "cta": None}

    # 2) Market overview
    mo_text = _first_text(paragraph_items[:1]) or _first_text(headline_items[:1])
    mo_bullets = _pick_by_tag(bullet_items, ["breadth", "flow", "intraday"], max_n=4)
    sec_map["market_overview"] = {"text": mo_text, "bullets": mo_bullets, "locks": [], "cta": None}

    # 3) Active sectors
    money_in_top = leaders.get("money_in_top") or []
    rs_top = leaders.get("rs20_top") or []
    if money_in_top:
        active_text = f"تا این لحظه، تمرکز جریان پول حقیقی بیشتر روی: {_join_top(money_in_top, 'sector', 3)}."
    elif rs_top:
        active_text = f"در نمای کوتاه‌مدت، گروه‌های قوی‌تر: {_join_top(rs_top, 'sector', 3)}."
    else:
        active_text = ""

    active_bullets = _pick_by_tag(bullet_items, ["rs", "daily", "flow"], max_n=4) or (bullet_items or [])[:2]
    sec_map["active_sectors"] = {
        "text": active_text,
        "bullets": active_bullets,
        "locks": ["symbol_level_flow", "intraday_rotation"] if mode == "public" else [],
        "cta": None,
    }

    # 4) Orderbook
    ob_text = "از نگاه دفتر سفارشات، نشانه‌های فشار خرید/فروش بررسی شد."
    ob_bullets = _pick_by_tag(bullet_items, ["orderbook"], max_n=4)
    if not ob_bullets:
        ob = (ms.get("orderbook") or {})
        st = ob.get("state")
        if st:
            ob_bullets = [_item(
                f"وضعیت کلی دفتر سفارش‌ها: «{st}».",
                evidence_refs=_evidence("signals.market_state.orderbook.state"),
                confidence=0.70,
                tags=["orderbook", "intraday"],
            )]
    sec_map["orderbook"] = {
        "text": ob_text,
        "bullets": ob_bullets,
        "locks": ["orderbook_microstructure", "spread_details"] if mode == "public" else [],
        "cta": None,
    }

    # 5) Anomalies
    an_bullets = _anomaly_items(anomalies, max_n=3)
    sec_map["anomalies"] = {
        "text": "هشدارهای کوتاه (در صورت وجود واگرایی بین سیگنال‌ها):" if an_bullets else "",
        "bullets": an_bullets,
        "locks": ["anomaly_pro"] if mode == "public" else [],
        "cta": _public_cta_after_anomalies() if mode == "public" else None,  # ✅ CTA 1 (consolidated)
    }

    # 6) Real/Legal
    nrv = _safe_int(((ms.get("flow") or {}).get("net_real_value")))
    if nrv is None:
        real_txt = ""
    else:
        real_txt = f"در جمع‌بندی، حقیقی‌ها در این لحظه {'خریدار' if nrv>0 else 'فروشنده' if nrv<0 else 'خنثی'} هستند."
    rl_bullets = _pick_by_tag(bullet_items, ["flow"], max_n=3)
    sec_map["real_legal"] = {
        "text": real_txt,
        "bullets": rl_bullets,
        "locks": ["real_legal_by_sector", "real_legal_by_symbol"] if mode == "public" else [],
        "cta": None,
    }

    # 7) History compare (RS + baseline)
    hc_text = _first_text(paragraph_items[1:2]) or "برای مقایسه با گذشته، به رهبران/عقب‌مانده‌ها و غیرعادی بودن نسبت به میانگین‌های تاریخی نگاه می‌کنیم."
    hc_bullets = _pick_by_tag(bullet_items, ["rs", "daily"], max_n=4)
    sec_map["history_compare"] = {
        "text": hc_text,
        "bullets": hc_bullets,
        "locks": ["baseline_zscores", "rs_5_20_60_full"] if mode == "public" else [],
        "cta": None,
    }

    # 8) Morning story
    ms_text = _first_text(paragraph_items[2:3]) or ""
    sec_map["morning_story"] = {
        "text": ms_text,
        "bullets": [],
        "locks": ["intraday_timeline", "rotation_story"] if mode == "public" else [],
        "cta": _public_cta_end_of_page() if mode == "public" else None,  # ✅ CTA 2 (consolidated)
    }

    # --- Emit ordered + enforce standards
    sections: List[Dict[str, Any]] = []
    for sid, title in SECTION_ORDER_GUEST:
        raw = sec_map.get(sid) or {"text": "", "bullets": [], "locks": [], "cta": None}
        sec = {
            "id": sid,
            "title": title,
            "text": raw.get("text") or "",
            "bullets": raw.get("bullets") or [],
            "locks": raw.get("locks") or [],
            "cta": raw.get("cta"),
        }
        sec = _enforce_section(
            sec,
            section_id=sid,
            is_header=(sid == "header"),
            max_bullets=(0 if sid == "header" else 4),
        )
        sections.append(sec)

    return sections


# ----------------------------
# Core: narrative builder
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
        "sections": [ ... 8 sections ... ],
        "headline": [ ... ],
        "bullets":  [ ... ],
        "paragraphs": [ ... ]
      }
    """

    ms = (signals or {}).get("market_state", {}) or {}
    leaders = (signals or {}).get("leaders", {}) or {}
    etf = (signals or {}).get("etf", {}) or {}
    anomalies = (signals or {}).get("anomalies", []) or []

    daily_date = ((meta or {}).get("asof", {}) or {}).get("daily_date")
    intraday_ts = ((meta or {}).get("asof", {}) or {}).get("intraday_ts")

    breadth = (ms.get("breadth") or {})
    flow = (ms.get("flow") or {})
    orderbook = (ms.get("orderbook") or {})

    green_ratio = _safe_float(breadth.get("green_ratio"))
    net_real_value = _safe_int(flow.get("net_real_value"))
    imbalance5 = _safe_float(orderbook.get("imbalance5"))
    imbalance_state = orderbook.get("state")

    regime = ms.get("regime")
    trend = ms.get("trend")
    conf = float(ms.get("confidence", 0.65))

    rs_top = leaders.get("rs20_top") or []
    rs_bottom = leaders.get("rs20_bottom") or []
    money_in_top = leaders.get("money_in_top") or []
    money_out_top = leaders.get("money_out_top") or []

    etf_available = bool(etf.get("available", False))
    etf_buckets = etf.get("buckets") or []

    headline_items: List[Dict[str, Any]] = []
    bullet_items: List[Dict[str, Any]] = []
    paragraph_items: List[Dict[str, Any]] = []

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

    # audience filter for legacy lists
    base = {"headline": headline_items, "bullets": bullet_items, "paragraphs": paragraph_items}

    if audience == "headline":
        base = {"headline": base["headline"], "bullets": [], "paragraphs": []}
    elif audience == "bullets":
        base = {"headline": base["headline"], "bullets": base["bullets"], "paragraphs": []}
    elif audience == "paragraphs":
        base = {"headline": base["headline"], "bullets": base["bullets"], "paragraphs": base["paragraphs"]}

    # ✅ sections are always generated for landing use
    sections = _build_sections(
        mode=mode,
        headline_items=base["headline"],
        bullet_items=base["bullets"],
        paragraph_items=base["paragraphs"],
        signals=signals or {},
    )

    return {
        "sections": sections,
        "headline": base["headline"],
        "bullets": base["bullets"],
        "paragraphs": base["paragraphs"],
    }


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
        pct = _pct(green_ratio * 100 if green_ratio <= 1 else green_ratio, 1) or ""
        out.append(_item(
            f"سهم نمادهای مثبت حدود {pct} بود.",
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

    if etf_available and etf_buckets:
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

    p1 = []
    p1.append("در آخرین وضعیت لحظه‌ای ثبت‌شده" if intraday_ts else "در وضعیت فعلی بازار")

    if green_ratio is not None:
        p1.append(f"سهم مثبت‌ها حدود {_pct(green_ratio*100 if green_ratio<=1 else green_ratio, 1)} بود")
    if net_real_value is not None:
        if net_real_value < 0:
            p1.append("و هم‌زمان خروج پول حقیقی دیده شد")
        elif net_real_value > 0:
            p1.append("و هم‌زمان ورود پول حقیقی دیده شد")
        else:
            p1.append("و جریان پول حقیقی خنثی بود")

    if imbalance_state:
        p1.append(f"و در دفتر سفارش‌ها نشانه‌ی «{imbalance_state}» گزارش شده است")

    out.append(_item(
        "، ".join([x for x in p1 if x]) + ".",
        evidence_refs=_evidence("signals.market_state"),
        confidence=min(0.88, max(0.55, conf)),
        tags=["paragraph", "intraday"],
    ))

    top_s = _join_top(rs_top, "sector", 3)
    bot_s = _join_top(rs_bottom, "sector", 3)

    p2 = []
    p2.append("در جمع‌بندی روزانه" if daily_date else "در جمع‌بندی")
    if top_s:
        p2.append(f"گروه‌های قوی‌تر (نسبت به بازار) شامل {top_s} بودند")
    if bot_s:
        p2.append(f"و گروه‌های ضعیف‌تر شامل {bot_s}")

    out.append(_item(
        "، ".join([x for x in p2 if x]) + ".",
        evidence_refs=_evidence("signals.leaders"),
        confidence=0.70,
        tags=["paragraph", "daily"],
    ))

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
# Templates: PRO (same style, concise)
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

    regime_txt = "Risk-off" if regime == "risk_off" else "Risk-on" if regime == "risk_on" else "Neutral"
    breadth_txt = f"breadth={_fmt_num(green_ratio, 3)}" if green_ratio is not None else ""
    flow_txt = f"real_flow={_fmt_money_rial(net_real_value)}" if net_real_value is not None else ""
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

    p1 = []
    p1.append(f"Intraday snapshot ({intraday_ts}) نشان می‌دهد" if intraday_ts else "Intraday snapshot نشان می‌دهد")
    if green_ratio is not None:
        p1.append(f"breadth با green_ratio={_fmt_num(green_ratio, 4)} است")
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

    top_s = _join_top(rs_top, "sector", 4)
    bot_s = _join_top(rs_bottom, "sector", 4)
    mi = _join_top(money_in_top, "sector", 3)
    mo = _join_top(money_out_top, "sector", 3)

    p2 = []
    p2.append(f"در نمای روزانه ({daily_date})" if daily_date else "در نمای روزانه")
    if top_s:
        p2.append(f"RS20 leaders شامل {top_s} هستند")
    if bot_s:
        p2.append(f"و laggards شامل {bot_s}")
    if mi:
        p2.append(f"بیشترین inflow در {mi}")
    if mo:
        p2.append(f"و بیشترین outflow در {mo}")
    out.append(_item(
        "؛ ".join([x for x in p2 if x]) + ".",
        evidence_refs=_evidence("signals.leaders"),
        confidence=0.74,
        tags=["paragraph", "daily", "pro"],
    ))

    if anomalies:
        out.append(_item(
            "هم‌چنین divergence بین برخی سیگنال‌ها دیده می‌شود (احتمال اختلاف زمان برداشت داده/تفاوت بین سفارش و معامله).",
            evidence_refs=_evidence("signals.anomalies"),
            confidence=0.68,
            tags=["paragraph", "anomaly", "pro"],
            severity="warn",
        ))

    return out[:3]