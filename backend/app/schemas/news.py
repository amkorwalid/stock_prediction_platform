from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class NewsArticle(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    article_id: str
    ticker_id: int
    symbol: str
    headline: str
    body_clean: str | None = None
    published_at: datetime
    market_session: Literal["pre_market", "intraday", "after_hours", "overnight"] | None = None
    source: str | None = None
    word_count: int | None = None
    lang: str | None = None


class PaginatedNews(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[NewsArticle]
