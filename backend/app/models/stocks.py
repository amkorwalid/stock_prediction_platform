from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, Numeric, SmallInteger, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class DimTicker(Base):
    __tablename__ = "dim_ticker"
    __table_args__ = {"schema": "data_warehouse"}

    ticker_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    company: Mapped[str] = mapped_column(String, nullable=False)
    sector: Mapped[str | None] = mapped_column(String)
    industry: Mapped[str | None] = mapped_column(String)
    exchange: Mapped[str] = mapped_column(String, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), default="USD")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    listed_at: Mapped[date | None] = mapped_column(Date)
    delisted_at: Mapped[date | None] = mapped_column(Date)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime)

    prices: Mapped[list["FactDailyPrices"]] = relationship("FactDailyPrices", back_populates="ticker")


class DimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "data_warehouse"}

    date_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    quarter: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    month: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    week_of_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    day_of_week: Mapped[str] = mapped_column(String(12), nullable=False)
    day_of_month: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    is_trading_day: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_month_end: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_quarter_end: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class FactDailyPrices(Base):
    __tablename__ = "fact_daily_prices"
    __table_args__ = (
        UniqueConstraint("ticker_id", "date_id", name="uq_fact_prices_ticker_date"),
        {"schema": "data_warehouse"},
    )

    price_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)
    open: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    adj_close: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    volume: Mapped[int] = mapped_column(BigInteger, nullable=False)
    vwap: Mapped[float | None] = mapped_column(Numeric(18, 6))
    created_at: Mapped[datetime | None] = mapped_column(DateTime)

    ticker: Mapped["DimTicker"] = relationship("DimTicker", back_populates="prices")
