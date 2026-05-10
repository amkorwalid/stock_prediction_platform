from datetime import date
from uuid import UUID

from fastapi import APIRouter, Query

from app.schemas import SentimentRow
from app.services import AnalysisService

router = APIRouter(prefix="/news-analysis", tags=["news-analysis"])


@router.get("", response_model=list[SentimentRow])
def list_news_analysis(
    symbol: str | None = None,
    date_from: date | None = Query(default=None, alias="from"),
    date_to: date | None = Query(default=None, alias="to"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[SentimentRow]:
    return AnalysisService.list_sentiment(symbol, date_from, date_to, limit, offset)


@router.get("/latest", response_model=list[SentimentRow])
def latest_news_analysis(symbol: str | None = None) -> list[SentimentRow]:
    return AnalysisService.latest_sentiment(symbol)


@router.get("/{article_id}", response_model=list[SentimentRow])
def get_analysis_for_article(article_id: UUID) -> list[SentimentRow]:
    return AnalysisService.by_article(article_id)
