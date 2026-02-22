# backend/api/commentary.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Optional, Literal, Dict, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.metadata import get_db
from backend.users.dependencies import require_permissions
from backend.utils.response import create_response
from backend.utils.logger import logger

from backend.commentary.composer import compose_commentary


router = APIRouter(prefix="/commentary", tags=["📝 Commentary"])


Mode = Literal["public", "pro"]
Audience = Literal["headline", "bullets", "paragraphs", "all"]


@router.get(
    "/daily-intraday",
    summary="Narrative ترکیبی Daily + Intraday (Rule-based, آماده LLM)",
)
async def get_daily_intraday_commentary(
    mode: Mode = Query("public", description="public یا pro"),
    audience: Audience = Query("all", description="headline | bullets | paragraphs | all"),
    sector_snapshot_limit: int = Query(10, ge=1, le=500, description="تعداد رکوردهای آخر sector_intraday_snapshot"),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Commentary.View", "ALL")),
):
    """
    خروجی ساختاریافته (JSON) برای:
    - سریع‌خوان (headline)
    - نیمه تحلیلی (bullets)
    - تحلیلی جدی (paragraphs)
    در دو نسخه public/pro
    """

    try:
        resp_model = await compose_commentary(
            db=db,
            mode=mode,
            audience=audience,
            sector_snapshot_limit=sector_snapshot_limit,
            llm_override=None,  # بعداً می‌تونی از این ورودی برای LLM استفاده کنی
        )

        # resp_model یک Pydantic model است
        payload = resp_model.model_dump()

        return create_response(
            data=payload,
            message="commentary generated",
        )

    except Exception as e:
        logger.exception("Error generating commentary: %s", e)
        return create_response(
            data=None,
            message="failed to generate commentary",
            error=str(e),
        )


@router.get(
    "/raw",
    summary="Raw bundle (facts + signals + narrative) برای دیباگ/تست",
)
async def get_commentary_raw_bundle(
    sector_snapshot_limit: int = Query(10, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    _=Depends(require_permissions("Commentary.Debug", "ALL")),
):
    """
    این endpoint برای خودت/تست مفیده:
    audience=all, mode=pro
    """
    try:
        resp_model = await compose_commentary(
            db=db,
            mode="pro",
            audience="all",
            sector_snapshot_limit=sector_snapshot_limit,
        )
        return create_response(
            data=resp_model.model_dump(),
            message="raw bundle generated",
        )
    except Exception as e:
        logger.exception("Error generating raw bundle: %s", e)
        return create_response(
            data=None,
            message="failed to generate raw bundle",
            error=str(e),
        )