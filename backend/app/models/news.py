from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, Enum, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

MarketSessionEnum = Enum(
    "pre_market", "intraday", "after_hours", "overnight",
    name="market_session_enum",
)


class FactNewsArticles(Base):
    __tablename__ = "fact_news_articles"
    __table_args__ = (
        {"schema": "data_warehouse"},
    )

    article_id: Mapped[UUID] = mapped_column(String, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    body_clean: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    market_session: Mapped[str | None] = mapped_column(String(20))
    source: Mapped[str | None] = mapped_column(String)
    lang: Mapped[str] = mapped_column(String(8), default="en")
    word_count: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)
