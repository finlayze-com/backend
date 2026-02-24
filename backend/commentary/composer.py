# backend/commentary/composer.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from backend.commentary.fetchers import fetch_facts_bundle
from backend.commentary.schemas import (
    Audience,
    CommentaryResponse,
    FactsBundle,
    IntradayFacts,
    DailyFacts,
    MetaBlock,
    AsOfBlock,
    Mode,
    NarrativeBundle,
    NarrativeItem,
    NarrativeSection,
    SignalsBundle,
)

from backend.commentary.signals import build_signals
from backend.commentary.narrative import build_narrative


def _to_narrative_bundle(n: Dict[str, Any]) -> NarrativeBundle:
    def conv_items(items):
        out = []
        for it in items or []:
            out.append(NarrativeItem(**it))
        return out

    def conv_sections(sections):
        out = []
        for s in sections or []:
            out.append(NarrativeSection(
                id=s.get("id", ""),
                title=s.get("title", ""),
                text=s.get("text", "") or "",
                bullets=conv_items(s.get("bullets") or []),
                locks=list(s.get("locks") or []),
                cta=s.get("cta"),
            ))
        return out

    return NarrativeBundle(
        sections=conv_sections(n.get("sections") or []),
        headline=conv_items(n.get("headline")),
        bullets=conv_items(n.get("bullets")),
        paragraphs=conv_items(n.get("paragraphs")),
    )


async def compose_commentary(
    *,
    db: AsyncSession,
    mode: Mode = "public",
    audience: Audience = "all",
    sector_snapshot_limit: int = 10,
    llm_override: Optional[Dict[str, Any]] = None,
) -> CommentaryResponse:
    """
    Hybrid pipeline:
      fetch facts (MV/snapshots) -> deterministic signals -> narrative -> JSON

    llm_override:
      برای آینده: می‌تونی اینجا خروجی LLM را بدهی تا signals یا narrative را override کند.
      فعلاً None بگذار.
    """

    # 1) fetch
    facts_raw = await fetch_facts_bundle(db, sector_snapshot_limit=sector_snapshot_limit)

    # 2) deterministic signals
    signals_raw = build_signals(facts_raw)

    # 3) (optional) LLM override layer (future)
    effective_signals = signals_raw
    if llm_override and isinstance(llm_override.get("signals"), dict):
        merged = dict(signals_raw)
        merged.update(llm_override["signals"])
        effective_signals = merged

    # 4) narrative (layered: public/pro × audience)
    meta = {
        "asof": {
            "daily_date": (facts_raw.get("daily", {}).get("asof", {}) or {}).get("date_miladi"),
            "intraday_ts": (facts_raw.get("intraday", {}).get("asof", {}) or {}).get("ts"),
            "intraday_day": (facts_raw.get("intraday", {}).get("asof", {}) or {}).get("snapshot_day"),
        }
    }

    narrative_raw = build_narrative(
        mode=mode,
        audience=audience,
        meta=meta,
        facts=facts_raw,
        signals=effective_signals,
    )

    if llm_override and isinstance(llm_override.get("narrative"), dict):
        narrative_raw = llm_override["narrative"]

    # 5) cast to schemas (Pydantic)
    asof = AsOfBlock(
        daily_date=meta["asof"].get("daily_date"),
        intraday_ts=meta["asof"].get("intraday_ts"),
        intraday_day=meta["asof"].get("intraday_day"),
    )
    meta_block = MetaBlock(asof=asof)

    facts_bundle = FactsBundle(
        daily=DailyFacts(**(facts_raw.get("daily") or {})),
        intraday=IntradayFacts(**(facts_raw.get("intraday") or {})),
    )

    resp = CommentaryResponse(
        mode=mode,
        audience=audience,
        meta=meta_block,
        facts=facts_bundle,
        signals=SignalsBundle(data=effective_signals),
        narrative=_to_narrative_bundle(narrative_raw),
        llm=llm_override,
    )
    return resp