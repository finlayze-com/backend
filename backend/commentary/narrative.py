# backend/commentary/narrative.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Literal


Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]


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


_SENT_SPLIT_RE = re.compile(r'(?<=[\.\!\؟\?])\s+')


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


def _pct(x: Optional[float], digits: int = 1) -> Optional[str]:
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


def _join_top(items: List[Dict[str, Any]], key: str = "sector", n: int = 3) -> str:
    out = []
    for it in (items or [])[:n]:
        v = it.get(key)
        if v:
            out.append(str(v))
    return "، ".join(out)


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


def _ensure_bullets(bullets: List[Dict[str, Any]], max_n: int = 4) -> List[Dict[str, Any]]:
    b = bullets or []
    return b[:max_n] if len(b) > max_n else b


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


def _cta_upgrade(message: str, target: str = "pro") -> Dict[str, Any]:
    return {"placement": "after_section", "message": message, "action": "upgrade", "target": target}


def _public_cta_after_anomalies() -> Dict[str, Any]:
    return _cta_upgrade(
        "برای دیدن جزئیات دقیق‌تر (چرخش پول بین صنایع، نام دقیق صنایع/نمادهای مقصد پول، و لایه‌های عمیق‌تر دفتر سفارش‌ها) نسخه حرفه‌ای را ببینید.",
        target="pro",
    )


def _public_cta_end_of_page() -> Dict[str, Any]:
    return _cta_upgrade(
        "نسخه حرفه‌ای: گزارش صنعت‌به‌صنعت + سهم‌به‌سهم، لیست غیرعادی‌ها (Z-score)، و روایت دقیق فازهای صبح تا الان.",
        target="pro",
    )


def _enforce_section(section: Dict[str, Any], section_id: str, is_header: bool) -> Dict[str, Any]:
    section["id"] = section.get("id") or section_id
    section["title"] = section.get("title") or ""
    section["text"] = section.get("text") or ""
    section["bullets"] = section.get("bullets") or []
    section["locks"] = section.get("locks") or []

    if is_header:
        section["text"] = _cap_lines(section["text"], 2)
    else:
        section["text"] = _cap_sentences(section["text"], 2)

    if not section["text"].strip():
        section["text"] = _fallback(section_id)

    if not is_header:
        section["bullets"] = _ensure_bullets(section["bullets"], 4)
    else:
        section["bullets"] = []

    return section


# ----------------------------
# Section builders from signals
# ----------------------------

def build_narrative(*, mode: Mode, audience: Audience, meta: Dict[str, Any], facts: Dict[str, Any], signals: Dict[str, Any]) -> Dict[str, Any]:
    ms = (signals or {}).get("market_state") or {}
    act = (signals or {}).get("active_sectors") or {}
    ob = (signals or {}).get("orderbook") or {}
    hc = (signals or {}).get("history_compare") or {}
    anoms = (signals or {}).get("anomalies") or []

    intraday_ts = ((meta or {}).get("asof", {}) or {}).get("intraday_ts")
    daily_date = ((meta or {}).get("asof", {}) or {}).get("daily_date")

    gr = _safe_float(((ms.get("breadth") or {}).get("green_ratio")))
    nrv = _safe_int(((ms.get("flow") or {}).get("net_real_value")))
    tv = _safe_int(((ms.get("value") or {}).get("total_value")))
    trend = ms.get("trend")
    regime = ms.get("regime")
    conf = float(ms.get("confidence", 0.65))

    # ---------- Header (2 lines)
    line1 = "خلاصه بازار (لحظه‌ای)"
    if trend == "bullish":
        line1 = "بازار این لحظه متمایل به تقاضا است."
    elif trend == "bearish":
        line1 = "بازار این لحظه متمایل به عرضه است."
    else:
        line1 = "بازار این لحظه متعادل و نوسانی است."

    line2_parts = []
    if intraday_ts:
        line2_parts.append(f"آخرین ثبت: {intraday_ts}")
    if gr is not None:
        line2_parts.append(f"پهنای مثبت‌ها: {_pct(gr*100, 1)}")
    if nrv is not None:
        line2_parts.append(f"پول حقیقی: {_fmt_money_rial(nrv)}")
    line2 = " | ".join(line2_parts) if line2_parts else ""

    # ---------- Market overview
    mo_text_parts = []
    mo_text_parts.append("در آخرین وضعیت ثبت‌شده")
    if tv is not None:
        mo_text_parts.append(f"ارزش معاملات حدود {_fmt_num(tv)}")
    if nrv is not None:
        mo_text_parts.append(f"و جریان پول حقیقی {'ورود' if nrv>0 else 'خروج' if nrv<0 else 'خنثی'} است")
    if gr is not None:
        mo_text_parts.append(f"(سهم مثبت‌ها {_pct(gr*100,1)})")
    mo_text = "، ".join([x for x in mo_text_parts if x]) + "."

    mo_bullets = []
    if regime:
        mo_bullets.append(_item(f"رژیم: {regime}", ["signals.market_state.regime"], conf, ["market","regime"]))
    if trend:
        mo_bullets.append(_item(f"جهت کلی: {trend}", ["signals.market_state.trend"], conf, ["market","trend"]))
    if tv is not None:
        mo_bullets.append(_item(f"ارزش معاملات: {_fmt_num(tv)}", ["signals.market_state.value.total_value"], 0.75, ["value"]))
    if nrv is not None:
        mo_bullets.append(_item(f"پول حقیقی: {_fmt_money_rial(nrv)}", ["signals.market_state.flow.net_real_value"], 0.75, ["flow"]))

    # ---------- Active sectors
    top_value = act.get("top_value_sectors") or []
    top_vol = act.get("top_volume_sectors") or []
    inflow = act.get("inflow_sectors") or []
    outflow = act.get("outflow_sectors") or []

    as_text = ""
    if top_value:
        as_text = f"تا این لحظه، بیشترین فعالیت به لحاظ ارزش معاملات در: {_join_top(top_value, 'sector', 3)} دیده می‌شود."
    elif inflow:
        as_text = f"تمرکز ورود پول حقیقی بیشتر روی: {_join_top(inflow, 'sector', 3)} است."

    as_bullets = []
    if top_value:
        as_bullets.append(_item(f"فعال‌ترین‌ها (ارزش): {_join_top(top_value,'sector',4)}", ["signals.active_sectors.top_value_sectors"], 0.72, ["active","value"]))
    if top_vol:
        as_bullets.append(_item(f"فعال‌ترین‌ها (حجم): {_join_top(top_vol,'sector',4)}", ["signals.active_sectors.top_volume_sectors"], 0.72, ["active","volume"]))
    if inflow:
        as_bullets.append(_item(f"ورود حقیقی: {_join_top(inflow,'sector',4)}", ["signals.active_sectors.inflow_sectors"], 0.70, ["flow","inflow"]))
    if outflow:
        as_bullets.append(_item(f"خروج حقیقی: {_join_top(outflow,'sector',4)}", ["signals.active_sectors.outflow_sectors"], 0.70, ["flow","outflow"]))

    # Public locks
    as_locks = ["exact_sector_ranking", "symbol_level_flow"] if mode == "public" else []

    # ---------- Orderbook
    buy_p = ob.get("buy_pressure_sectors") or []
    sell_p = ob.get("sell_pressure_sectors") or []
    spread_w = ob.get("spread_wide_sectors") or []
    soft_acc = ob.get("soft_accumulation") or []
    trap_buy = ob.get("trap_buy_hint") or []
    heavy_dist = ob.get("heavy_distribution") or []

    # نتیجه‌گیری متن Orderbook
    ob_focus = "بالانس"
    if soft_acc:
        ob_focus = "جمع‌آوری نرم"
    elif heavy_dist:
        ob_focus = "عرضه سنگین"
    elif buy_p and not sell_p:
        ob_focus = "متمایل به خرید"
    elif sell_p and not buy_p:
        ob_focus = "متمایل به فروش"

    ob_text = f"دفتر سفارش‌ها نشان می‌دهد تمرکز «{ob_focus}» بیشتر در چند گروه دیده می‌شود."
    ob_bullets = []
    if buy_p:
        ob_bullets.append(_item(f"فشار خرید: {_join_top(buy_p,'sector',3)}", ["signals.orderbook.buy_pressure_sectors"], 0.70, ["orderbook","buy"]))
    if sell_p:
        ob_bullets.append(_item(f"فشار فروش: {_join_top(sell_p,'sector',3)}", ["signals.orderbook.sell_pressure_sectors"], 0.70, ["orderbook","sell"]))
    if spread_w:
        ob_bullets.append(_item(f"اسپرد غیرعادی: {_join_top(spread_w,'sector',3)}", ["signals.orderbook.spread_wide_sectors"], 0.68, ["orderbook","spread"]))
    if trap_buy:
        ob_bullets.append(_item(f"هشدار تله خرید/نیاز به تأیید: {_join_top(trap_buy,'sector',2)}", ["signals.orderbook.trap_buy_hint"], 0.62, ["orderbook","warn"], "warn"))

    ob_locks = ["orderbook_microstructure", "spread_details"] if mode == "public" else []

    # ---------- Anomalies
    an_bullets = []
    for a in (anoms or [])[:3]:
        an_bullets.append(_item(
            a.get("text") or a.get("code") or "هشدار",
            a.get("evidence_refs") or ["signals.anomalies"],
            0.65,
            ["anomaly"],
            a.get("severity") or "warn"
        ))
    an_text = "هشدارهای کوتاه (در صورت واگرایی بین سیگنال‌ها):" if an_bullets else ""

    # CTA after anomalies (public)
    an_cta = _public_cta_after_anomalies() if mode == "public" else None

    # ---------- Real/Legal
    rl_text = ""
    if nrv is not None:
        rl_text = f"در جمع‌بندی، حقیقی‌ها در این لحظه {'خریدار' if nrv>0 else 'فروشنده' if nrv<0 else 'خنثی'} هستند."
    rl_bullets = []
    if nrv is not None:
        rl_bullets.append(_item(f"خالص پول حقیقی: {_fmt_money_rial(nrv)}", ["signals.market_state.flow.net_real_value"], 0.75, ["flow","real_legal"]))

    rl_locks = ["real_legal_by_sector", "real_legal_by_symbol"] if mode == "public" else []

    # ---------- History compare (RS + Z)
    rs_top = hc.get("rs20_top") or []
    rs_bot = hc.get("rs20_bottom") or []
    zv_pos = hc.get("z_value_pos") or []
    zv_neg = hc.get("z_value_neg") or []
    zr_pos = hc.get("z_real_pos") or []
    zr_neg = hc.get("z_real_neg") or []

    hc_text = "در قیاس تاریخی، ترکیب قدرت نسبی (RS20) و غیرعادی بودن (Z) برای تشخیص لیدرهای پایدار/واگرایی‌ها استفاده می‌شود."
    hc_bullets = []
    if rs_top:
        hc_bullets.append(_item(f"RS20 قوی‌ترها: {_join_top(rs_top,'sector',4)}", ["signals.history_compare.rs20_top"], 0.70, ["rs"]))
    if rs_bot:
        hc_bullets.append(_item(f"RS20 ضعیف‌ترها: {_join_top(rs_bot,'sector',4)}", ["signals.history_compare.rs20_bottom"], 0.70, ["rs"]))
    if zr_pos:
        hc_bullets.append(_item(f"غیرعادی مثبت پول حقیقی (Z_real+): {_join_top(zr_pos,'sector',3)}", ["signals.history_compare.z_real_pos"], 0.68, ["z_real"]))
    if zr_neg:
        hc_bullets.append(_item(f"غیرعادی منفی پول حقیقی (Z_real-): {_join_top(zr_neg,'sector',3)}", ["signals.history_compare.z_real_neg"], 0.68, ["z_real"]))

    hc_locks = ["baseline_zscores", "rs_5_20_60_full", "sector_cards"] if mode == "public" else []

    # ---------- Morning story (timeline later)
    ms_text = "روایت زمانی دقیق نیازمند سری زمانی intraday است (در فاز بعدی اضافه می‌شود)."
    ms_cta = _public_cta_end_of_page() if mode == "public" else None

    # ---------- Build sections map
    sec_map: Dict[str, Dict[str, Any]] = {
        "header": {"text": f"{line1}\n{line2}".strip(), "bullets": [], "locks": [], "cta": None},
        "market_overview": {"text": mo_text, "bullets": mo_bullets, "locks": [], "cta": None},
        "active_sectors": {"text": as_text, "bullets": as_bullets, "locks": as_locks, "cta": None},
        "orderbook": {"text": ob_text, "bullets": ob_bullets, "locks": ob_locks, "cta": None},
        "anomalies": {"text": an_text, "bullets": an_bullets, "locks": (["anomaly_pro"] if mode=="public" else []), "cta": an_cta},
        "real_legal": {"text": rl_text, "bullets": rl_bullets, "locks": rl_locks, "cta": None},
        "history_compare": {"text": hc_text, "bullets": hc_bullets, "locks": hc_locks, "cta": None},
        "morning_story": {"text": ms_text, "bullets": [], "locks": (["intraday_timeline","rotation_story"] if mode=="public" else []), "cta": ms_cta},
    }

    # ---------- Enforce + order
    sections: List[Dict[str, Any]] = []
    for sid, title in SECTION_ORDER_GUEST:
        raw = sec_map.get(sid) or {}
        sec = {
            "id": sid,
            "title": title,
            "text": raw.get("text") or "",
            "bullets": raw.get("bullets") or [],
            "locks": raw.get("locks") or [],
            "cta": raw.get("cta"),
        }
        sections.append(_enforce_section(sec, sid, is_header=(sid=="header")))

    # legacy lists for compatibility (optional)
    headline = [_item(line1, ["signals.market_state"], conf, ["headline"]),
                _item(line2, ["signals.market_state"], conf, ["headline"])][:2]
    bullets = (mo_bullets + as_bullets + ob_bullets + an_bullets + rl_bullets + hc_bullets)[:12]
    paragraphs = [_item(mo_text, ["signals.market_state"], conf, ["paragraph"]),
                  _item(hc_text, ["signals.history_compare"], 0.7, ["paragraph"])][:3]

    # audience filter (optional)
    if audience == "headline":
        bullets, paragraphs = [], []
    elif audience == "bullets":
        paragraphs = []
    elif audience == "paragraphs":
        pass

    return {
        "sections": sections,
        "headline": headline,
        "bullets": bullets,
        "paragraphs": paragraphs,
    }