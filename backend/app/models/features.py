from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class FeatTechnical(Base):
    __tablename__ = "feat_technical"
    __table_args__ = (
        UniqueConstraint("ticker_id", "date_id", name="uq_feat_tech_ticker_date"),
        {"schema": "data_warehouse"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)

    sma_5: Mapped[float | None] = mapped_column(Numeric(18, 6))
    sma_10: Mapped[float | None] = mapped_column(Numeric(18, 6))
    sma_20: Mapped[float | None] = mapped_column(Numeric(18, 6))
    sma_50: Mapped[float | None] = mapped_column(Numeric(18, 6))
    ema_12: Mapped[float | None] = mapped_column(Numeric(18, 6))
    ema_26: Mapped[float | None] = mapped_column(Numeric(18, 6))
    rsi_14: Mapped[float | None] = mapped_column(Numeric(8, 4))
    macd: Mapped[float | None] = mapped_column(Numeric(18, 8))
    macd_signal: Mapped[float | None] = mapped_column(Numeric(18, 8))
    macd_hist: Mapped[float | None] = mapped_column(Numeric(18, 8))
    boll_upper: Mapped[float | None] = mapped_column(Numeric(18, 6))
    boll_lower: Mapped[float | None] = mapped_column(Numeric(18, 6))
    boll_mid: Mapped[float | None] = mapped_column(Numeric(18, 6))
    atr_14: Mapped[float | None] = mapped_column(Numeric(18, 6))
    obv: Mapped[int | None] = mapped_column(BigInteger)
    vwap: Mapped[float | None] = mapped_column(Numeric(18, 6))
    computed_at: Mapped[datetime | None] = mapped_column(DateTime)


class FeatNewsSentiment(Base):
    __tablename__ = "feat_news_sentiment"
    __table_args__ = (
        UniqueConstraint("ticker_id", "date_id", name="uq_feat_sent_ticker_date"),
        {"schema": "data_warehouse"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)
    article_id: Mapped[str] = mapped_column(String, nullable=False)
    analysis_summary: Mapped[str | None] = mapped_column(Text)
    sentiment_score: Mapped[float | None] = mapped_column(Numeric(6, 4))
    sentiment_label: Mapped[str | None] = mapped_column(String(12))
    relevance: Mapped[int | None] = mapped_column(Integer)
    computed_at: Mapped[datetime | None] = mapped_column(DateTime)
