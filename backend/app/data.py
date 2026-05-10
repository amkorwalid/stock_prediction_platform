from __future__ import annotations

from datetime import date, datetime, timedelta
from uuid import UUID

from app.schemas import (
    ClassificationPrediction,
    ModelPerformance,
    ModelRegistryEntry,
    NewsArticle,
    OHLCVBar,
    RegressionPrediction,
    SentimentRow,
    TechnicalFeatures,
    Ticker,
)

NOW = datetime(2026, 5, 10, 0, 0, 0)

TICKERS = [
    Ticker(
        ticker_id=1,
        symbol="AAPL",
        company="Apple Inc.",
        sector="Technology",
        industry="Consumer Electronics",
        exchange="NASDAQ",
        currency="USD",
        is_active=True,
        listed_at=date(1980, 12, 12),
        updated_at=NOW,
    ),
    Ticker(
        ticker_id=2,
        symbol="MSFT",
        company="Microsoft Corporation",
        sector="Technology",
        industry="Software",
        exchange="NASDAQ",
        currency="USD",
        is_active=True,
        listed_at=date(1986, 3, 13),
        updated_at=NOW,
    ),
]

PRICES: dict[str, list[OHLCVBar]] = {
    "AAPL": [
        OHLCVBar(
            trade_date=date(2026, 5, 8),
            open=189.1,
            high=191.2,
            low=188.4,
            close=190.3,
            adj_close=190.3,
            volume=52_400_000,
            vwap=189.95,
        ),
        OHLCVBar(
            trade_date=date(2026, 5, 9),
            open=190.5,
            high=192.0,
            low=189.8,
            close=191.7,
            adj_close=191.7,
            volume=48_200_000,
            vwap=191.1,
        ),
    ],
    "MSFT": [
        OHLCVBar(
            trade_date=date(2026, 5, 9),
            open=415.1,
            high=418.2,
            low=413.0,
            close=417.6,
            adj_close=417.6,
            volume=22_300_000,
            vwap=416.2,
        )
    ],
}

TECHNICALS: dict[str, list[TechnicalFeatures]] = {
    "AAPL": [
        TechnicalFeatures(
            trade_date=date(2026, 5, 9),
            sma_5=189.2,
            sma_10=187.8,
            sma_20=185.1,
            sma_50=180.3,
            ema_12=188.6,
            ema_26=186.4,
            rsi_14=57.8,
            macd=1.2,
            macd_signal=1.0,
            macd_hist=0.2,
            boll_upper=193.0,
            boll_lower=177.2,
            boll_mid=185.1,
            atr_14=3.2,
            obv=1_254_300_000,
            vwap=191.1,
        )
    ],
    "MSFT": [
        TechnicalFeatures(
            trade_date=date(2026, 5, 9),
            sma_5=412.4,
            sma_10=409.7,
            sma_20=404.2,
            sma_50=398.5,
            ema_12=413.0,
            ema_26=408.3,
            rsi_14=61.3,
            macd=2.7,
            macd_signal=2.1,
            macd_hist=0.6,
            boll_upper=421.4,
            boll_lower=387.0,
            boll_mid=404.2,
            atr_14=5.1,
            obv=980_000_000,
            vwap=416.2,
        )
    ],
}

ARTICLES = [
    NewsArticle(
        article_id=UUID("11111111-1111-1111-1111-111111111111"),
        ticker_id=1,
        symbol="AAPL",
        headline="Apple reports stronger iPhone demand",
        body_clean="Apple reported stronger than expected demand in key regions.",
        published_at=NOW - timedelta(hours=14),
        market_session="after_hours",
        source="Alpha Vantage",
        word_count=74,
        lang="en",
    ),
    NewsArticle(
        article_id=UUID("22222222-2222-2222-2222-222222222222"),
        ticker_id=2,
        symbol="MSFT",
        headline="Microsoft expands AI cloud offerings",
        body_clean="Microsoft announced expanded enterprise AI capabilities.",
        published_at=NOW - timedelta(days=1),
        market_session="intraday",
        source="Alpha Vantage",
        word_count=61,
        lang="en",
    ),
]

SENTIMENT_ROWS = [
    SentimentRow(
        article_id=UUID("11111111-1111-1111-1111-111111111111"),
        symbol="AAPL",
        trade_date=date(2026, 5, 9),
        article_count=5,
        pre_mkt_count=1,
        after_hrs_count=2,
        sentiment_score=0.42,
        sentiment_label="Positive",
        sentiment_std=0.17,
        score_tier1=0.48,
        avg_relevance=0.86,
        computed_at=NOW,
    ),
    SentimentRow(
        article_id=UUID("22222222-2222-2222-2222-222222222222"),
        symbol="MSFT",
        trade_date=date(2026, 5, 9),
        article_count=4,
        pre_mkt_count=0,
        after_hrs_count=1,
        sentiment_score=0.31,
        sentiment_label="Positive",
        sentiment_std=0.21,
        score_tier1=0.34,
        avg_relevance=0.81,
        computed_at=NOW,
    ),
]

REGRESSION_PREDICTIONS = [
    RegressionPrediction(
        pred_id=UUID("33333333-3333-3333-3333-333333333333"),
        symbol="AAPL",
        prediction_date=date(2026, 5, 9),
        target_date=date(2026, 5, 12),
        horizon_days=1,
        model_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        pred_close=192.8,
        pred_return=0.0057,
        pred_log_return=0.0056,
        confidence_lo=191.1,
        confidence_hi=194.6,
        predicted_at=NOW,
    ),
    RegressionPrediction(
        pred_id=UUID("44444444-4444-4444-4444-444444444444"),
        symbol="MSFT",
        prediction_date=date(2026, 5, 9),
        target_date=date(2026, 5, 12),
        horizon_days=1,
        model_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        pred_close=420.1,
        pred_return=0.0060,
        pred_log_return=0.0059,
        confidence_lo=416.9,
        confidence_hi=423.2,
        predicted_at=NOW,
    ),
]

CLASSIFICATION_PREDICTIONS = [
    ClassificationPrediction(
        pred_id=UUID("55555555-5555-5555-5555-555555555555"),
        symbol="AAPL",
        prediction_date=date(2026, 5, 9),
        target_date=date(2026, 5, 12),
        horizon_days=1,
        model_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        pred_direction=1,
        prob_up=0.62,
        prob_flat=0.25,
        prob_down=0.13,
        pred_confidence=0.62,
        is_high_confidence=True,
        predicted_at=NOW,
    ),
    ClassificationPrediction(
        pred_id=UUID("66666666-6666-6666-6666-666666666666"),
        symbol="MSFT",
        prediction_date=date(2026, 5, 9),
        target_date=date(2026, 5, 12),
        horizon_days=1,
        model_id=UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        pred_direction=1,
        prob_up=0.58,
        prob_flat=0.29,
        prob_down=0.13,
        pred_confidence=0.58,
        is_high_confidence=True,
        predicted_at=NOW,
    ),
]

MODEL_REGISTRY = [
    ModelRegistryEntry(
        model_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        model_name="xgboost_v3_regression",
        model_type="regression",
        version="3.1.0",
        horizon_days=1,
        algorithm="XGBoost",
        feature_set="price+news",
        training_start_date=date(2023, 1, 1),
        training_end_date=date(2026, 4, 30),
        val_metric_name="mae",
        val_metric_value=1.82,
        is_champion=True,
        registered_at=NOW,
    ),
    ModelRegistryEntry(
        model_id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        model_name="xgboost_v3_classification",
        model_type="classification",
        version="3.1.0",
        horizon_days=1,
        algorithm="XGBoost",
        feature_set="price+news",
        training_start_date=date(2023, 1, 1),
        training_end_date=date(2026, 4, 30),
        val_metric_name="f1_score",
        val_metric_value=0.64,
        is_champion=True,
        registered_at=NOW,
    ),
]

MODEL_PERFORMANCE = [
    ModelPerformance(
        perf_id=UUID("77777777-7777-7777-7777-777777777777"),
        model_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        symbol=None,
        eval_date=date(2026, 5, 9),
        horizon_days=1,
        eval_window_days=90,
        n_samples=1250,
        mae=1.9,
        rmse=2.7,
        mape=1.6,
        r2_score=0.69,
        drift_flag=False,
        drift_severity="none",
        evaluated_at=NOW,
    )
]
