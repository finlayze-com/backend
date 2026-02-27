# backend/commentary/schemas.py

from __future__ import annotations
from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]

# ----------------------------
# Meta
# ----------------------------

class AsOfBlock(BaseModel):
    daily_date: Optional[date] = None
    intraday_ts: Optional[datetime] = None
    intraday_day: Optional[date] = None


class MetaBlock(BaseModel):
    asof: AsOfBlock = Field(default_factory=AsOfBlock)
    source: str = "mv+snapshots"
    notes: List[str] = Field(default_factory=list)


# ----------------------------
# Facts
# ----------------------------

class DailyFacts(BaseModel):
    asof: Dict[str, Any] = Field(default_factory=dict)
    sector_daily_latest: List[Dict[str, Any]] = Field(default_factory=list)
    sector_rs_latest: List[Dict[str, Any]] = Field(default_factory=list)
    sector_baseline_latest: List[Dict[str, Any]] = Field(default_factory=list)
    market_daily_latest: List[Dict[str, Any]] = Field(default_factory=list)


class IntradayFacts(BaseModel):
    asof: Dict[str, Any] = Field(default_factory=dict)

    # snapshot
    market_snapshot: Dict[str, Any] = Field(default_factory=dict)
    sector_rows_at_ts: List[Dict[str, Any]] = Field(default_factory=list)

    # full series (timeline ready)
    market_series: List[Dict[str, Any]] = Field(default_factory=list)
    sector_series: List[Dict[str, Any]] = Field(default_factory=list)

    # materialized views
    mv_live_sector_report: Dict[str, Any] = Field(default_factory=dict)
    mv_orderbook_report: Dict[str, Any] = Field(default_factory=dict)


class FactsBundle(BaseModel):
    daily: DailyFacts = Field(default_factory=DailyFacts)
    intraday: IntradayFacts = Field(default_factory=IntradayFacts)


class SignalsBundle(BaseModel):
    data: Dict[str, Any] = Field(default_factory=dict)


# ----------------------------
# Narrative
# ----------------------------

class NarrativeItem(BaseModel):
    text: str
    evidence_refs: List[str] = Field(default_factory=list)
    confidence: float = 0.7
    tags: List[str] = Field(default_factory=list)
    severity: Optional[str] = None


class NarrativeSection(BaseModel):
    id: str
    title: str
    text: str = ""
    bullets: List[NarrativeItem] = Field(default_factory=list)
    locks: List[str] = Field(default_factory=list)
    cta: Optional[Dict[str, Any]] = None


class NarrativeBundle(BaseModel):
    sections: List[NarrativeSection] = Field(default_factory=list)
    headline: List[NarrativeItem] = Field(default_factory=list)
    bullets: List[NarrativeItem] = Field(default_factory=list)
    paragraphs: List[NarrativeItem] = Field(default_factory=list)


class CommentaryResponse(BaseModel):
    mode: Mode
    audience: Audience
    meta: MetaBlock
    facts: FactsBundle
    signals: SignalsBundle
    narrative: NarrativeBundle
    llm: Optional[Dict[str, Any]] = None