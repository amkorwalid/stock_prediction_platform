from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.features import FeatNewsSentiment
from app.models.stocks import DimDate, DimTicker
from app.schemas.sentiment import SentimentRow

_LABEL_MAP = {"UP": "Positive", "DOWN": "Negative", "CONSTANT": "Neutral"}


def _to_row(sentiment: FeatNewsSentiment, symbol: str, trade_date: date) -> SentimentRow:
    label = _LABEL_MAP.get(sentiment.sentiment_label or "", sentiment.sentiment_label)
    return SentimentRow(
        symbol=symbol,
        trade_date=trade_date,
        sentiment_score=float(sentiment.sentiment_score) if sentiment.sentiment_score is not None else None,
        sentiment_label=label,
        avg_relevance=float(sentiment.relevance) if sentiment.relevance is not None else None,
        computed_at=sentiment.computed_at,
    )


async def list_sentiment(
    db: AsyncSession,
    symbol: str | None,
    from_date: date | None,
    to_date: date | None,
    limit: int,
    offset: int,
) -> list[SentimentRow]:
    stmt = (
        select(FeatNewsSentiment, DimTicker.symbol, DimDate.trade_date)
        .join(DimTicker, FeatNewsSentiment.ticker_id == DimTicker.ticker_id)
        .join(DimDate, FeatNewsSentiment.date_id == DimDate.date_id)
    )
    if symbol:
        stmt = stmt.where(DimTicker.symbol == symbol.upper())
    if from_date:
        stmt = stmt.where(DimDate.trade_date >= from_date)
    if to_date:
        stmt = stmt.where(DimDate.trade_date <= to_date)

    stmt = stmt.order_by(DimDate.trade_date.desc()).offset(offset).limit(limit)
    rows = (await db.execute(stmt)).all()
    return [_to_row(r.FeatNewsSentiment, r.symbol, r.trade_date) for r in rows]


async def latest_sentiment(db: AsyncSession, symbol: str | None) -> list[SentimentRow]:
    stmt = (
        select(FeatNewsSentiment, DimTicker.symbol, DimDate.trade_date)
        .join(DimTicker, FeatNewsSentiment.ticker_id == DimTicker.ticker_id)
        .join(DimDate, FeatNewsSentiment.date_id == DimDate.date_id)
    )
    if symbol:
        stmt = stmt.where(DimTicker.symbol == symbol.upper())
    stmt = stmt.order_by(DimDate.trade_date.desc())
    rows = (await db.execute(stmt)).all()
    return [_to_row(r.FeatNewsSentiment, r.symbol, r.trade_date) for r in rows]


async def get_sentiment_by_article(db: AsyncSession, article_id: str) -> list[SentimentRow] | None:
    stmt = (
        select(FeatNewsSentiment, DimTicker.symbol, DimDate.trade_date)
        .join(DimTicker, FeatNewsSentiment.ticker_id == DimTicker.ticker_id)
        .join(DimDate, FeatNewsSentiment.date_id == DimDate.date_id)
        .where(FeatNewsSentiment.article_id == article_id)
    )
    rows = (await db.execute(stmt)).all()
    if not rows:
        return None
    return [_to_row(r.FeatNewsSentiment, r.symbol, r.trade_date) for r in rows]
