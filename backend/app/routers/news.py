from datetime import date
from uuid import UUID

from fastapi import APIRouter, Query

from app.schemas import NewsArticle, PaginatedNews
from app.services import NewsService

router = APIRouter(prefix="/news", tags=["news"])


@router.get("", response_model=PaginatedNews)
def list_news(
    symbol: str | None = None,
    date_from: date | None = Query(default=None, alias="from"),
    date_to: date | None = Query(default=None, alias="to"),
    market_session: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> PaginatedNews:
    return NewsService.list_articles(symbol, date_from, date_to, market_session, limit, offset)


@router.get("/{article_id}", response_model=NewsArticle)
def get_article(article_id: UUID) -> NewsArticle:
    return NewsService.get_article(article_id)
