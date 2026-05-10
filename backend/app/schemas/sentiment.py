from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class SentimentRow(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    symbol: str
    trade_date: date
    article_count: int | None = None
    pre_mkt_count: int | None = None
    after_hrs_count: int | None = None
    sentiment_score: float | None = None
    sentiment_label: Literal["Positive", "Neutral", "Negative"] | None = None
    sentiment_std: float | None = None
    score_tier1: float | None = None
    avg_relevance: float | None = None
    computed_at: datetime | None = None
