from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.news import FactNewsArticles
from app.models.stocks import DimDate, DimTicker
from app.schemas.news import NewsArticle, PaginatedNews


async def list_news(
    db: AsyncSession,
    symbol: str | None,
    from_date: date | None,
    to_date: date | None,
    market_session: str | None,
    limit: int,
    offset: int,
) -> PaginatedNews:
    stmt = select(FactNewsArticles, DimTicker.symbol).join(
        DimTicker, FactNewsArticles.ticker_id == DimTicker.ticker_id
    )
    if symbol:
        stmt = stmt.where(DimTicker.symbol == symbol.upper())
    if market_session:
        stmt = stmt.where(FactNewsArticles.market_session == market_session)
    if from_date or to_date:
        stmt = stmt.join(DimDate, FactNewsArticles.date_id == DimDate.date_id)
        if from_date:
            stmt = stmt.where(DimDate.trade_date >= from_date)
        if to_date:
            stmt = stmt.where(DimDate.trade_date <= to_date)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = (await db.execute(count_stmt)).scalar_one()

    stmt = stmt.order_by(FactNewsArticles.published_at.desc()).offset(offset).limit(limit)
    rows = (await db.execute(stmt)).all()

    items = [
        NewsArticle(
            article_id=str(r.FactNewsArticles.article_id),
            ticker_id=r.FactNewsArticles.ticker_id,
            symbol=r.symbol,
            headline=r.FactNewsArticles.headline,
            body_clean=r.FactNewsArticles.body_clean,
            published_at=r.FactNewsArticles.published_at,
            market_session=r.FactNewsArticles.market_session,
            source=r.FactNewsArticles.source,
            word_count=r.FactNewsArticles.word_count,
            lang=r.FactNewsArticles.lang,
        )
        for r in rows
    ]
    return PaginatedNews(total=total, limit=limit, offset=offset, items=items)


async def get_article(db: AsyncSession, article_id: str) -> NewsArticle | None:
    stmt = (
        select(FactNewsArticles, DimTicker.symbol)
        .join(DimTicker, FactNewsArticles.ticker_id == DimTicker.ticker_id)
        .where(FactNewsArticles.article_id == article_id)
    )
    row = (await db.execute(stmt)).first()
    if not row:
        return None
    return NewsArticle(
        article_id=str(row.FactNewsArticles.article_id),
        ticker_id=row.FactNewsArticles.ticker_id,
        symbol=row.symbol,
        headline=row.FactNewsArticles.headline,
        body_clean=row.FactNewsArticles.body_clean,
        published_at=row.FactNewsArticles.published_at,
        market_session=row.FactNewsArticles.market_session,
        source=row.FactNewsArticles.source,
        word_count=row.FactNewsArticles.word_count,
        lang=row.FactNewsArticles.lang,
    )
