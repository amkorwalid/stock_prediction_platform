from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class Error(BaseModel):
    detail: str
    code: str | None = None


class Ticker(BaseModel):
    ticker_id: int
    symbol: str
    company: str
    sector: str | None = None
    industry: str | None = None
    exchange: str
    currency: str = "USD"
    is_active: bool = True
    listed_at: date | None = None
    updated_at: datetime


class PaginatedTickers(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[Ticker]


class OHLCVBar(BaseModel):
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int
    vwap: float | None = None


class TechnicalFeatures(BaseModel):
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


class NewsArticle(BaseModel):
    article_id: UUID
    ticker_id: int
    symbol: str
    headline: str
    body_clean: str | None = None
    published_at: datetime
    market_session: Literal["pre_market", "intraday", "after_hours", "overnight"]
    source: str
    word_count: int | None = None
    lang: str = "en"


class PaginatedNews(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[NewsArticle]


class SentimentRow(BaseModel):
    article_id: UUID
    symbol: str
    trade_date: date
    article_count: int
    pre_mkt_count: int
    after_hrs_count: int
    sentiment_score: float
    sentiment_label: Literal["Positive", "Neutral", "Negative"]
    sentiment_std: float | None = None
    score_tier1: float | None = None
    avg_relevance: float | None = None
    computed_at: datetime


class RegressionPrediction(BaseModel):
    pred_id: UUID
    symbol: str
    prediction_date: date
    target_date: date
    horizon_days: Literal[1, 5, 20]
    model_id: UUID
    pred_close: float
    pred_return: float
    pred_log_return: float
    confidence_lo: float
    confidence_hi: float
    actual_close: float | None = None
    actual_return: float | None = None
    mae: float | None = None
    predicted_at: datetime


class ClassificationPrediction(BaseModel):
    pred_id: UUID
    symbol: str
    prediction_date: date
    target_date: date
    horizon_days: Literal[1, 5, 20]
    model_id: UUID
    pred_direction: Literal[1, 0, -1]
    prob_up: float
    prob_flat: float
    prob_down: float
    pred_confidence: float
    is_high_confidence: bool
    actual_direction: int | None = None
    is_correct: bool | None = None
    predicted_at: datetime


class PredictionsResponse(BaseModel):
    symbol: str
    regression: list[RegressionPrediction]
    classification: list[ClassificationPrediction]


class LatestPrediction(BaseModel):
    symbol: str
    regression: dict[str, RegressionPrediction]
    classification: dict[str, ClassificationPrediction]


class ModelRegistryEntry(BaseModel):
    model_id: UUID
    model_name: str
    model_type: Literal["regression", "classification"]
    version: str
    horizon_days: Literal[1, 5, 20]
    algorithm: str
    feature_set: Literal["price_only", "price+news", "news_only"]
    training_start_date: date
    training_end_date: date
    val_metric_name: str | None = None
    val_metric_value: float | None = None
    is_champion: bool
    registered_at: datetime


class ModelPerformance(BaseModel):
    perf_id: UUID
    model_id: UUID
    symbol: str | None = None
    eval_date: date
    horizon_days: int
    eval_window_days: int | None = None
    n_samples: int
    mae: float | None = None
    rmse: float | None = None
    mape: float | None = None
    r2_score: float | None = None
    directional_accuracy: float | None = None
    accuracy: float | None = None
    precision_score: float | None = None
    recall: float | None = None
    f1_score: float | None = None
    auc_roc: float | None = None
    matthews_corr: float | None = None
    drift_flag: bool
    drift_severity: Literal["none", "warning", "critical"]
    evaluated_at: datetime
