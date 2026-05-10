from datetime import date, datetime

from pydantic import BaseModel, ConfigDict


class Ticker(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ticker_id: int
    symbol: str
    company: str
    sector: str | None = None
    industry: str | None = None
    exchange: str
    currency: str | None = None
    is_active: bool
    listed_at: date | None = None
    updated_at: datetime | None = None


class PaginatedTickers(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[Ticker]


class OHLCVBar(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int
    vwap: float | None = None


class TechnicalFeatures(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    trade_date: date
    sma_5: float | None = None
    sma_10: float | None = None
    sma_20: float | None = None
    sma_50: float | None = None
    ema_12: float | None = None
    ema_26: float | None = None
    rsi_14: float | None = None
    macd: float | None = None
    macd_signal: float | None = None
    macd_hist: float | None = None
    boll_upper: float | None = None
    boll_lower: float | None = None
    boll_mid: float | None = None
    atr_14: float | None = None
    obv: int | None = None
    vwap: float | None = None
