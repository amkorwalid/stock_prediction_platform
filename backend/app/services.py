from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import HTTPException

from app import data
from app.schemas import (
    ClassificationPrediction,
    LatestPrediction,
    ModelPerformance,
    ModelRegistryEntry,
    NewsArticle,
    OHLCVBar,
    PaginatedNews,
    PaginatedTickers,
    PredictionsResponse,
    RegressionPrediction,
    SentimentRow,
    TechnicalFeatures,
    Ticker,
)


class StockService:
    @staticmethod
    def list_tickers(
        sector: str | None,
        exchange: str | None,
        is_active: bool,
        limit: int,
        offset: int,
    ) -> PaginatedTickers:
        items = [t for t in data.TICKERS if t.is_active == is_active]
        if sector:
            items = [t for t in items if t.sector and t.sector.lower() == sector.lower()]
        if exchange:
            items = [t for t in items if t.exchange.lower() == exchange.lower()]

        total = len(items)
        page = items[offset : offset + limit]
        return PaginatedTickers(total=total, limit=limit, offset=offset, items=page)

    @staticmethod
    def get_ticker(symbol: str) -> Ticker:
        for ticker in data.TICKERS:
            if ticker.symbol == symbol.upper():
                return ticker
        raise HTTPException(status_code=404, detail="Ticker not found")

    @staticmethod
    def list_prices(symbol: str, date_from: date, date_to: date | None) -> list[OHLCVBar]:
        StockService.get_ticker(symbol)
        rows = data.PRICES.get(symbol.upper(), [])
        if date_to is None:
            date_to = date.max
        return [r for r in rows if date_from <= r.trade_date <= date_to]

    @staticmethod
    def list_technicals(
        symbol: str,
        date_from: date | None,
        date_to: date | None,
    ) -> list[TechnicalFeatures]:
        StockService.get_ticker(symbol)
        rows = data.TECHNICALS.get(symbol.upper(), [])
        if date_from:
            rows = [r for r in rows if r.trade_date >= date_from]
        if date_to:
            rows = [r for r in rows if r.trade_date <= date_to]
        return rows


class NewsService:
    @staticmethod
    def list_articles(
        symbol: str | None,
        date_from: date | None,
        date_to: date | None,
        market_session: str | None,
        limit: int,
        offset: int,
    ) -> PaginatedNews:
        items = data.ARTICLES
        if symbol:
            items = [n for n in items if n.symbol == symbol.upper()]
        if date_from:
            items = [n for n in items if n.published_at.date() >= date_from]
        if date_to:
            items = [n for n in items if n.published_at.date() <= date_to]
        if market_session:
            items = [n for n in items if n.market_session == market_session]

        total = len(items)
        return PaginatedNews(total=total, limit=limit, offset=offset, items=items[offset : offset + limit])

    @staticmethod
    def get_article(article_id: UUID) -> NewsArticle:
        for article in data.ARTICLES:
            if article.article_id == article_id:
                return article
        raise HTTPException(status_code=404, detail="Article not found")


class AnalysisService:
    @staticmethod
    def list_sentiment(
        symbol: str | None,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
    ) -> list[SentimentRow]:
        rows = data.SENTIMENT_ROWS
        if symbol:
            rows = [r for r in rows if r.symbol == symbol.upper()]
        if date_from:
            rows = [r for r in rows if r.trade_date >= date_from]
        if date_to:
            rows = [r for r in rows if r.trade_date <= date_to]
        return rows[offset : offset + limit]

    @staticmethod
    def latest_sentiment(symbol: str | None) -> list[SentimentRow]:
        rows = data.SENTIMENT_ROWS
        if symbol:
            rows = [r for r in rows if r.symbol == symbol.upper()]
        return sorted(rows, key=lambda r: r.computed_at, reverse=True)

    @staticmethod
    def by_article(article_id: UUID) -> list[SentimentRow]:
        rows = [r for r in data.SENTIMENT_ROWS if r.article_id == article_id]
        if not rows:
            raise HTTPException(status_code=404, detail="Sentiment analysis not found")
        return rows


class PredictionService:
    @staticmethod
    def list_predictions(
        symbol: str,
        horizon_days: int | None,
        model_type: str,
        date_from: date | None,
        date_to: date | None,
        limit: int,
    ) -> PredictionsResponse:
        symbol = symbol.upper()
        StockService.get_ticker(symbol)

        regression = [r for r in data.REGRESSION_PREDICTIONS if r.symbol == symbol]
        classification = [r for r in data.CLASSIFICATION_PREDICTIONS if r.symbol == symbol]

        if horizon_days:
            regression = [r for r in regression if r.horizon_days == horizon_days]
            classification = [c for c in classification if c.horizon_days == horizon_days]
        if date_from:
            regression = [r for r in regression if r.prediction_date >= date_from]
            classification = [c for c in classification if c.prediction_date >= date_from]
        if date_to:
            regression = [r for r in regression if r.prediction_date <= date_to]
            classification = [c for c in classification if c.prediction_date <= date_to]

        if model_type == "regression":
            classification = []
        elif model_type == "classification":
            regression = []

        return PredictionsResponse(
            symbol=symbol,
            regression=regression[:limit],
            classification=classification[:limit],
        )

    @staticmethod
    def latest(symbol: str) -> LatestPrediction:
        symbol = symbol.upper()
        StockService.get_ticker(symbol)

        regression: dict[str, RegressionPrediction] = {}
        classification: dict[str, ClassificationPrediction] = {}

        for row in sorted(
            [r for r in data.REGRESSION_PREDICTIONS if r.symbol == symbol],
            key=lambda r: r.predicted_at,
            reverse=True,
        ):
            regression.setdefault(str(row.horizon_days), row)

        for row in sorted(
            [c for c in data.CLASSIFICATION_PREDICTIONS if c.symbol == symbol],
            key=lambda c: c.predicted_at,
            reverse=True,
        ):
            classification.setdefault(str(row.horizon_days), row)

        if not regression and not classification:
            raise HTTPException(status_code=404, detail="Predictions not found")

        return LatestPrediction(symbol=symbol, regression=regression, classification=classification)

    @staticmethod
    def list_models(model_type: str | None) -> list[ModelRegistryEntry]:
        rows = data.MODEL_REGISTRY
        if model_type:
            rows = [m for m in rows if m.model_type == model_type]
        return rows

    @staticmethod
    def model_performance(
        model_id: UUID,
        horizon_days: int | None,
        eval_window_days: int | None,
    ) -> list[ModelPerformance]:
        rows = [p for p in data.MODEL_PERFORMANCE if p.model_id == model_id]
        if horizon_days:
            rows = [p for p in rows if p.horizon_days == horizon_days]
        if eval_window_days:
            rows = [p for p in rows if p.eval_window_days == eval_window_days]
        return rows
