from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.sentiment import SentimentRow
from app.services import analysis_service

router = APIRouter(prefix="/news-analysis", tags=["news-analysis"])


@router.get("", response_model=list[SentimentRow])
async def list_sentiment(
    symbol: str | None = Query(None),
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
):
    return await analysis_service.list_sentiment(db, symbol, from_, to, limit, offset)


@router.get("/latest", response_model=list[SentimentRow])
async def latest_sentiment(
    symbol: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await analysis_service.latest_sentiment(db, symbol)


@router.get("/{article_id}", response_model=list[SentimentRow])
async def get_sentiment_by_article(article_id: str, db: AsyncSession = Depends(get_db)):
    rows = await analysis_service.get_sentiment_by_article(db, article_id)
    if rows is None:
        raise HTTPException(status_code=404, detail="Sentiment analysis not found for this article")
    return rows
