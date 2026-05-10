from __future__ import annotations

from datetime import date
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

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

MARKET_SESSION_SQL = """
CASE
    WHEN CAST({published_at} AS time) < TIME '09:30:00' THEN 'pre_market'
    WHEN CAST({published_at} AS time) < TIME '16:00:00' THEN 'intraday'
    WHEN CAST({published_at} AS time) < TIME '20:00:00' THEN 'after_hours'
    ELSE 'overnight'
END
"""


def _fetch_all(session: Session, sql: str, params: dict) -> list[dict]:
    return [dict(row) for row in session.execute(text(sql), params).mappings().all()]


def _fetch_one(session: Session, sql: str, params: dict) -> dict | None:
    row = session.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def _normalize_sentiment_label(label: str | None) -> str:
    mapping = {'UP': 'Positive', 'DOWN': 'Negative', 'CONSTANT': 'Neutral'}
    return mapping.get((label or '').upper(), 'Neutral')


def _sentiment_rows(session: Session, where_clause: str, params: dict, order_clause: str) -> list[dict]:
    return _fetch_all(
        session,
        f"""
        SELECT fns.article_id, dt.symbol, dd.trade_date,
               agg.article_count, agg.pre_mkt_count, agg.after_hrs_count,
               fns.sentiment_score, fns.sentiment_label,
               NULL::double precision AS sentiment_std,
               NULL::double precision AS score_tier1,
               CASE WHEN fns.relevance IS NULL THEN NULL ELSE fns.relevance / 100.0 END AS avg_relevance,
               fns.computed_at
        FROM data_warehouse.feat_news_sentiment fns
        JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fns.ticker_id
        JOIN data_warehouse.dim_date dd ON dd.date_id = fns.date_id
        JOIN LATERAL (
            SELECT COUNT(*) AS article_count,
                   COUNT(*) FILTER (
                       WHERE {MARKET_SESSION_SQL.format(published_at='fna.published_at')} = 'pre_market'
                   ) AS pre_mkt_count,
                   COUNT(*) FILTER (
                       WHERE {MARKET_SESSION_SQL.format(published_at='fna.published_at')} = 'after_hours'
                   ) AS after_hrs_count
            FROM data_warehouse.fact_news_articles fna
            WHERE fna.ticker_id = fns.ticker_id
              AND fna.date_id = fns.date_id
        ) agg ON TRUE
        {where_clause}
        {order_clause}
        """,
        params,
    )


class StockService:
    @staticmethod
    def list_tickers(
        session: Session,
        sector: str | None,
        exchange: str | None,
        is_active: bool,
        limit: int,
        offset: int,
    ) -> PaginatedTickers:
        params = {
            'sector': sector,
            'exchange': exchange,
            'is_active': is_active,
            'limit': limit,
            'offset': offset,
        }
        total = _fetch_one(
            session,
            """
            SELECT COUNT(*) AS total
            FROM data_warehouse.dim_ticker
            WHERE is_active = :is_active
              AND (:sector IS NULL OR sector = :sector)
              AND (:exchange IS NULL OR exchange = :exchange)
            """,
            params,
        )['total']
        items = [
            Ticker.model_validate(row)
            for row in _fetch_all(
                session,
                """
                SELECT ticker_id, symbol, company, sector, industry, exchange, currency,
                       is_active, listed_at, updated_at
                FROM data_warehouse.dim_ticker
                WHERE is_active = :is_active
                  AND (:sector IS NULL OR sector = :sector)
                  AND (:exchange IS NULL OR exchange = :exchange)
                ORDER BY symbol
                LIMIT :limit OFFSET :offset
                """,
                params,
            )
        ]
        return PaginatedTickers(total=total, limit=limit, offset=offset, items=items)

    @staticmethod
    def get_ticker(session: Session, symbol: str) -> Ticker:
        row = _fetch_one(
            session,
            """
            SELECT ticker_id, symbol, company, sector, industry, exchange, currency,
                   is_active, listed_at, updated_at
            FROM data_warehouse.dim_ticker
            WHERE symbol = :symbol
            LIMIT 1
            """,
            {'symbol': symbol.upper()},
        )
        if row:
            return Ticker.model_validate(row)
        raise HTTPException(status_code=404, detail='Ticker not found')

    @staticmethod
    def list_prices(
        session: Session,
        symbol: str,
        date_from: date,
        date_to: date | None,
    ) -> list[OHLCVBar]:
        StockService.get_ticker(session, symbol)
        rows = _fetch_all(
            session,
            """
            SELECT dd.trade_date, fdp.open, fdp.high, fdp.low, fdp.close,
                   fdp.adj_close, fdp.volume, fdp.vwap
            FROM data_warehouse.fact_daily_prices fdp
            JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fdp.ticker_id
            JOIN data_warehouse.dim_date dd ON dd.date_id = fdp.date_id
            WHERE dt.symbol = :symbol
              AND dd.trade_date >= :date_from
              AND (:date_to IS NULL OR dd.trade_date <= :date_to)
            ORDER BY dd.trade_date
            """,
            {'symbol': symbol.upper(), 'date_from': date_from, 'date_to': date_to},
        )
        return [OHLCVBar.model_validate(row) for row in rows]

    @staticmethod
    def list_technicals(
        session: Session,
        symbol: str,
        date_from: date | None,
        date_to: date | None,
    ) -> list[TechnicalFeatures]:
        StockService.get_ticker(session, symbol)
        rows = _fetch_all(
            session,
            """
            SELECT dd.trade_date, ft.sma_5, ft.sma_10, ft.sma_20, ft.sma_50,
                   ft.ema_12, ft.ema_26, ft.rsi_14, ft.macd, ft.macd_signal,
                   ft.macd_hist, ft.boll_upper, ft.boll_lower, ft.boll_mid,
                   ft.atr_14, ft.obv, ft.vwap
            FROM data_warehouse.feat_technical ft
            JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = ft.ticker_id
            JOIN data_warehouse.dim_date dd ON dd.date_id = ft.date_id
            WHERE dt.symbol = :symbol
              AND (:date_from IS NULL OR dd.trade_date >= :date_from)
              AND (:date_to IS NULL OR dd.trade_date <= :date_to)
            ORDER BY dd.trade_date
            """,
            {'symbol': symbol.upper(), 'date_from': date_from, 'date_to': date_to},
        )
        return [TechnicalFeatures.model_validate(row) for row in rows]


class NewsService:
    @staticmethod
    def list_articles(
        session: Session,
        symbol: str | None,
        date_from: date | None,
        date_to: date | None,
        market_session: str | None,
        limit: int,
        offset: int,
    ) -> PaginatedNews:
        market_session_case = MARKET_SESSION_SQL.format(published_at='fna.published_at')
        params = {
            'symbol': symbol.upper() if symbol else None,
            'date_from': date_from,
            'date_to': date_to,
            'market_session': market_session,
            'limit': limit,
            'offset': offset,
        }
        total = _fetch_one(
            session,
            f"""
            SELECT COUNT(*) AS total
            FROM data_warehouse.fact_news_articles fna
            JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fna.ticker_id
            WHERE (:symbol IS NULL OR dt.symbol = :symbol)
              AND (:date_from IS NULL OR fna.published_at::date >= :date_from)
              AND (:date_to IS NULL OR fna.published_at::date <= :date_to)
              AND (:market_session IS NULL OR {market_session_case} = :market_session)
            """,
            params,
        )['total']
        items = [
            NewsArticle.model_validate(row)
            for row in _fetch_all(
                session,
                f"""
                SELECT fna.article_id, fna.ticker_id, dt.symbol, fna.headline, fna.body_clean,
                       fna.published_at, {market_session_case} AS market_session,
                       COALESCE(rna.source, 'unknown') AS source, fna.word_count, fna.lang
                FROM data_warehouse.fact_news_articles fna
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fna.ticker_id
                LEFT JOIN data_warehouse.raw_news_articles rna ON rna.article_id = fna.article_id
                WHERE (:symbol IS NULL OR dt.symbol = :symbol)
                  AND (:date_from IS NULL OR fna.published_at::date >= :date_from)
                  AND (:date_to IS NULL OR fna.published_at::date <= :date_to)
                  AND (:market_session IS NULL OR {market_session_case} = :market_session)
                ORDER BY fna.published_at DESC
                LIMIT :limit OFFSET :offset
                """,
                params,
            )
        ]
        return PaginatedNews(total=total, limit=limit, offset=offset, items=items)

    @staticmethod
    def get_article(session: Session, article_id: UUID) -> NewsArticle:
        market_session_case = MARKET_SESSION_SQL.format(published_at='fna.published_at')
        row = _fetch_one(
            session,
            f"""
            SELECT fna.article_id, fna.ticker_id, dt.symbol, fna.headline, fna.body_clean,
                   fna.published_at, {market_session_case} AS market_session,
                   COALESCE(rna.source, 'unknown') AS source, fna.word_count, fna.lang
            FROM data_warehouse.fact_news_articles fna
            JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fna.ticker_id
            LEFT JOIN data_warehouse.raw_news_articles rna ON rna.article_id = fna.article_id
            WHERE fna.article_id = :article_id
            LIMIT 1
            """,
            {'article_id': article_id},
        )
        if row:
            return NewsArticle.model_validate(row)
        raise HTTPException(status_code=404, detail='Article not found')


class AnalysisService:
    @staticmethod
    def list_sentiment(
        session: Session,
        symbol: str | None,
        date_from: date | None,
        date_to: date | None,
        limit: int,
        offset: int,
    ) -> list[SentimentRow]:
        rows = _sentiment_rows(
            session,
            """
            WHERE (:symbol IS NULL OR dt.symbol = :symbol)
              AND (:date_from IS NULL OR dd.trade_date >= :date_from)
              AND (:date_to IS NULL OR dd.trade_date <= :date_to)
            """,
            {
                'symbol': symbol.upper() if symbol else None,
                'date_from': date_from,
                'date_to': date_to,
                'limit': limit,
                'offset': offset,
            },
            'ORDER BY dd.trade_date DESC, dt.symbol LIMIT :limit OFFSET :offset',
        )
        return [
            SentimentRow.model_validate({**row, 'sentiment_label': _normalize_sentiment_label(row['sentiment_label'])})
            for row in rows
        ]

    @staticmethod
    def latest_sentiment(session: Session, symbol: str | None) -> list[SentimentRow]:
        rows = _fetch_all(
            session,
            f"""
            WITH ranked AS (
                SELECT fns.article_id, dt.symbol, dd.trade_date,
                       agg.article_count, agg.pre_mkt_count, agg.after_hrs_count,
                       fns.sentiment_score, fns.sentiment_label,
                       NULL::double precision AS sentiment_std,
                       NULL::double precision AS score_tier1,
                       CASE WHEN fns.relevance IS NULL THEN NULL ELSE fns.relevance / 100.0 END AS avg_relevance,
                       fns.computed_at,
                       ROW_NUMBER() OVER (PARTITION BY dt.symbol ORDER BY fns.computed_at DESC) AS row_num
                FROM data_warehouse.feat_news_sentiment fns
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = fns.ticker_id
                JOIN data_warehouse.dim_date dd ON dd.date_id = fns.date_id
                JOIN LATERAL (
                    SELECT COUNT(*) AS article_count,
                           COUNT(*) FILTER (
                               WHERE {MARKET_SESSION_SQL.format(published_at='fna.published_at')} = 'pre_market'
                           ) AS pre_mkt_count,
                           COUNT(*) FILTER (
                               WHERE {MARKET_SESSION_SQL.format(published_at='fna.published_at')} = 'after_hours'
                           ) AS after_hrs_count
                    FROM data_warehouse.fact_news_articles fna
                    WHERE fna.ticker_id = fns.ticker_id
                      AND fna.date_id = fns.date_id
                ) agg ON TRUE
                WHERE (:symbol IS NULL OR dt.symbol = :symbol)
            )
            SELECT article_id, symbol, trade_date, article_count, pre_mkt_count, after_hrs_count,
                   sentiment_score, sentiment_label, sentiment_std, score_tier1,
                   avg_relevance, computed_at
            FROM ranked
            WHERE row_num = 1
            ORDER BY computed_at DESC, symbol
            """,
            {'symbol': symbol.upper() if symbol else None},
        )
        return [
            SentimentRow.model_validate({**row, 'sentiment_label': _normalize_sentiment_label(row['sentiment_label'])})
            for row in rows
        ]

    @staticmethod
    def by_article(session: Session, article_id: UUID) -> list[SentimentRow]:
        rows = _sentiment_rows(
            session,
            'WHERE fns.article_id = :article_id',
            {'article_id': article_id},
            'ORDER BY fns.computed_at DESC',
        )
        if not rows:
            raise HTTPException(status_code=404, detail='Sentiment analysis not found')
        return [
            SentimentRow.model_validate({**row, 'sentiment_label': _normalize_sentiment_label(row['sentiment_label'])})
            for row in rows
        ]


class PredictionService:
    @staticmethod
    def list_predictions(
        session: Session,
        symbol: str,
        horizon_days: int | None,
        model_type: str,
        date_from: date | None,
        date_to: date | None,
        limit: int,
    ) -> PredictionsResponse:
        symbol = symbol.upper()
        StockService.get_ticker(session, symbol)

        regression = [
            RegressionPrediction.model_validate(row)
            for row in _fetch_all(
                session,
                """
                SELECT pr.pred_id, dt.symbol, pr.prediction_date, pr.target_date, pr.horizon_days,
                       pr.model_id, pr.pred_close, pr.pred_return, pr.pred_log_return,
                       pr.confidence_lo, pr.confidence_hi, pr.actual_close, pr.actual_return,
                       pr.mae, pr.predicted_at
                FROM data_warehouse.predictions_regression pr
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = pr.ticker_id
                WHERE dt.symbol = :symbol
                  AND (:horizon_days IS NULL OR pr.horizon_days = :horizon_days)
                  AND (:date_from IS NULL OR pr.prediction_date >= :date_from)
                  AND (:date_to IS NULL OR pr.prediction_date <= :date_to)
                ORDER BY pr.predicted_at DESC
                LIMIT :limit
                """,
                {
                    'symbol': symbol,
                    'horizon_days': horizon_days,
                    'date_from': date_from,
                    'date_to': date_to,
                    'limit': limit,
                },
            )
        ]
        classification = [
            ClassificationPrediction.model_validate(row)
            for row in _fetch_all(
                session,
                """
                SELECT pc.pred_id, dt.symbol, pc.prediction_date, pc.target_date, pc.horizon_days,
                       pc.model_id, pc.pred_direction, pc.prob_up, pc.prob_flat, pc.prob_down,
                       pc.pred_confidence, pc.is_high_confidence, pc.actual_direction,
                       pc.is_correct, pc.predicted_at
                FROM data_warehouse.predictions_classification pc
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = pc.ticker_id
                WHERE dt.symbol = :symbol
                  AND (:horizon_days IS NULL OR pc.horizon_days = :horizon_days)
                  AND (:date_from IS NULL OR pc.prediction_date >= :date_from)
                  AND (:date_to IS NULL OR pc.prediction_date <= :date_to)
                ORDER BY pc.predicted_at DESC
                LIMIT :limit
                """,
                {
                    'symbol': symbol,
                    'horizon_days': horizon_days,
                    'date_from': date_from,
                    'date_to': date_to,
                    'limit': limit,
                },
            )
        ]

        if model_type == 'regression':
            classification = []
        elif model_type == 'classification':
            regression = []

        return PredictionsResponse(symbol=symbol, regression=regression, classification=classification)

    @staticmethod
    def latest(session: Session, symbol: str) -> LatestPrediction:
        symbol = symbol.upper()
        StockService.get_ticker(session, symbol)

        regression = {
            str(row['horizon_days']): RegressionPrediction.model_validate(row)
            for row in _fetch_all(
                session,
                """
                SELECT DISTINCT ON (pr.horizon_days)
                       pr.pred_id, dt.symbol, pr.prediction_date, pr.target_date, pr.horizon_days,
                       pr.model_id, pr.pred_close, pr.pred_return, pr.pred_log_return,
                       pr.confidence_lo, pr.confidence_hi, pr.actual_close, pr.actual_return,
                       pr.mae, pr.predicted_at
                FROM data_warehouse.predictions_regression pr
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = pr.ticker_id
                WHERE dt.symbol = :symbol
                ORDER BY pr.horizon_days, pr.predicted_at DESC
                """,
                {'symbol': symbol},
            )
        }
        classification = {
            str(row['horizon_days']): ClassificationPrediction.model_validate(row)
            for row in _fetch_all(
                session,
                """
                SELECT DISTINCT ON (pc.horizon_days)
                       pc.pred_id, dt.symbol, pc.prediction_date, pc.target_date, pc.horizon_days,
                       pc.model_id, pc.pred_direction, pc.prob_up, pc.prob_flat, pc.prob_down,
                       pc.pred_confidence, pc.is_high_confidence, pc.actual_direction,
                       pc.is_correct, pc.predicted_at
                FROM data_warehouse.predictions_classification pc
                JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = pc.ticker_id
                WHERE dt.symbol = :symbol
                ORDER BY pc.horizon_days, pc.predicted_at DESC
                """,
                {'symbol': symbol},
            )
        }

        if not regression and not classification:
            raise HTTPException(status_code=404, detail='Predictions not found')

        return LatestPrediction(symbol=symbol, regression=regression, classification=classification)

    @staticmethod
    def list_models(session: Session, model_type: str | None) -> list[ModelRegistryEntry]:
        rows = _fetch_all(
            session,
            """
            SELECT model_id, model_name, model_type, version, horizon_days, algorithm,
                   feature_set, training_start_date, training_end_date,
                   NULL::varchar AS val_metric_name,
                   NULL::double precision AS val_metric_value,
                   is_champion, registered_at
            FROM data_warehouse.model_registry
            WHERE (:model_type IS NULL OR model_type = :model_type)
            ORDER BY is_champion DESC, registered_at DESC
            """,
            {'model_type': model_type},
        )
        return [ModelRegistryEntry.model_validate(row) for row in rows]

    @staticmethod
    def model_performance(
        session: Session,
        model_id: UUID,
        horizon_days: int | None,
        eval_window_days: int | None,
    ) -> list[ModelPerformance]:
        rows = _fetch_all(
            session,
            """
            SELECT mp.perf_id, mp.model_id, dt.symbol,
                   mp.evaluated_at::date AS eval_date,
                   mp.horizon_days,
                   CASE
                       WHEN mp.eval_start_date IS NOT NULL AND mp.eval_end_date IS NOT NULL
                       THEN (mp.eval_end_date - mp.eval_start_date + 1)
                       ELSE NULL
                   END AS eval_window_days,
                   mp.n_samples,
                   rmp.mae,
                   rmp.rmse,
                   NULL::double precision AS mape,
                   NULL::double precision AS r2_score,
                   NULL::double precision AS directional_accuracy,
                   cmp.accuracy,
                   cmp.precision_score,
                   cmp.recall,
                   cmp.f1_score,
                   cmp.auc_roc,
                   NULL::double precision AS matthews_corr,
                   FALSE AS drift_flag,
                   'none' AS drift_severity,
                   mp.evaluated_at
            FROM data_warehouse.model_performance mp
            LEFT JOIN data_warehouse.dim_ticker dt ON dt.ticker_id = mp.ticker_id
            LEFT JOIN data_warehouse.regression_model_performance rmp ON rmp.perf_id = mp.perf_id
            LEFT JOIN data_warehouse.classification_model_performance cmp ON cmp.perf_id = mp.perf_id
            WHERE mp.model_id = :model_id
              AND (:horizon_days IS NULL OR mp.horizon_days = :horizon_days)
              AND (
                    :eval_window_days IS NULL OR (
                        mp.eval_start_date IS NOT NULL
                        AND mp.eval_end_date IS NOT NULL
                        AND (mp.eval_end_date - mp.eval_start_date + 1) = :eval_window_days
                    )
              )
            ORDER BY mp.evaluated_at DESC
            """,
            {
                'model_id': model_id,
                'horizon_days': horizon_days,
                'eval_window_days': eval_window_days,
            },
        )
        return [ModelPerformance.model_validate(row) for row in rows]
