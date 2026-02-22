# backend/commentary/schemas.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]


# ----------------------------
# Core blocks
# ----------------------------

class AsOfBlock(BaseModel):
    daily_date: Optional[date] = None
    intraday_ts: Optional[datetime] = None
    intraday_day: Optional[date] = None


class MetaBlock(BaseModel):
    asof: AsOfBlock = Field(default_factory=AsOfBlock)
    source: str = "mv+snapshots"
    notes: List[str] = Field(default_factory=list)


class DailyFacts(BaseModel):
    asof: Dict[str, Any] = Field(default_factory=dict)
    sector_daily_latest: List[Dict[str, Any]] = Field(default_factory=list)
    sector_rs_latest: List[Dict[str, Any]] = Field(default_factory=list)
    market_daily_latest: List[Dict[str, Any]] = Field(default_factory=list)


class IntradayFacts(BaseModel):
    asof: Dict[str, Any] = Field(default_factory=dict)
    market_snapshot: Dict[str, Any] = Field(default_factory=dict)
    sector_snapshots: List[Dict[str, Any]] = Field(default_factory=list)


class FactsBundle(BaseModel):
    daily: DailyFacts = Field(default_factory=DailyFacts)
    intraday: IntradayFacts = Field(default_factory=IntradayFacts)


# signals.py خروجی‌اش Dict است و بعداً ممکن است LLM آن را override کند.
class SignalsBundle(BaseModel):
    data: Dict[str, Any] = Field(default_factory=dict)


# narrative.py خروجی‌اش dict شامل headline/bullets/paragraphs است
class NarrativeItem(BaseModel):
    text: str
    evidence_refs: List[str] = Field(default_factory=list)
    confidence: float = 0.7
    tags: List[str] = Field(default_factory=list)
    severity: Optional[str] = None


class NarrativeBundle(BaseModel):
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

    # جا برای آینده (LLM / human edits / feedback loop)
    llm: Optional[Dict[str, Any]] = None