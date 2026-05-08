from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.news import NewsArticle, PaginatedNews
from app.services import news_service

router = APIRouter(prefix="/news", tags=["news"])


@router.get("", response_model=PaginatedNews)
async def list_news(
    symbol: str | None = Query(None),
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    market_session: Literal["pre_market", "intraday", "after_hours", "overnight"] | None = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
):
    return await news_service.list_news(db, symbol, from_, to, market_session, limit, offset)


@router.get("/{article_id}", response_model=NewsArticle)
async def get_article(article_id: str, db: AsyncSession = Depends(get_db)):
    article = await news_service.get_article(db, article_id)
    if not article:
        raise HTTPException(status_code=404, detail="Article not found")
    return article
