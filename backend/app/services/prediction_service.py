from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.predictions import (
    ClassificationModelPerformance,
    ModelPerformance,
    ModelRegistry,
    PredictionsClassification,
    PredictionsRegression,
    RegressionModelPerformance,
)
from app.models.stocks import DimDate, DimTicker
from app.schemas.predictions import (
    ClassificationPrediction,
    LatestPrediction,
    ModelPerformance as ModelPerformanceSchema,
    ModelRegistryEntry,
    PredictionsResponse,
    RegressionPrediction,
)


def _build_regression(row, symbol: str) -> RegressionPrediction:
    p: PredictionsRegression = row.PredictionsRegression
    return RegressionPrediction(
        pred_id=p.pred_id,
        symbol=symbol,
        prediction_date=p.prediction_date,
        target_date=p.target_date,
        horizon_days=p.horizon_days,
        model_id=p.model_id,
        pred_close=float(p.pred_close) if p.pred_close is not None else None,
        pred_return=float(p.pred_return) if p.pred_return is not None else None,
        pred_log_return=float(p.pred_log_return) if p.pred_log_return is not None else None,
        confidence_lo=float(p.confidence_lo) if p.confidence_lo is not None else None,
        confidence_hi=float(p.confidence_hi) if p.confidence_hi is not None else None,
        actual_close=float(p.actual_close) if p.actual_close is not None else None,
        actual_return=float(p.actual_return) if p.actual_return is not None else None,
        mae=float(p.mae) if p.mae is not None else None,
        predicted_at=p.predicted_at,
    )


def _build_classification(row, symbol: str) -> ClassificationPrediction:
    p: PredictionsClassification = row.PredictionsClassification
    return ClassificationPrediction(
        pred_id=p.pred_id,
        symbol=symbol,
        prediction_date=p.prediction_date,
        target_date=p.target_date,
        horizon_days=p.horizon_days,
        model_id=p.model_id,
        pred_direction=p.pred_direction,
        prob_up=float(p.prob_up),
        prob_flat=float(p.prob_flat),
        prob_down=float(p.prob_down),
        pred_confidence=float(p.pred_confidence) if p.pred_confidence is not None else None,
        is_high_confidence=p.is_high_confidence,
        actual_direction=p.actual_direction,
        is_correct=p.is_correct,
        predicted_at=p.predicted_at,
    )


async def _resolve_ticker(db: AsyncSession, symbol: str) -> DimTicker | None:
    stmt = select(DimTicker).where(DimTicker.symbol == symbol.upper())
    return (await db.execute(stmt)).scalar_one_or_none()


async def get_predictions(
    db: AsyncSession,
    symbol: str,
    horizon_days: int | None,
    model_type: str,
    from_date: date | None,
    to_date: date | None,
    limit: int,
) -> PredictionsResponse | None:
    ticker = await _resolve_ticker(db, symbol)
    if not ticker:
        return None

    regression: list[RegressionPrediction] = []
    classification: list[ClassificationPrediction] = []

    if model_type in ("regression", "all"):
        stmt = (
            select(PredictionsRegression, DimDate.trade_date)
            .join(DimDate, PredictionsRegression.date_id == DimDate.date_id)
            .where(PredictionsRegression.ticker_id == ticker.ticker_id)
        )
        if horizon_days:
            stmt = stmt.where(PredictionsRegression.horizon_days == horizon_days)
        if from_date:
            stmt = stmt.where(PredictionsRegression.prediction_date >= from_date)
        if to_date:
            stmt = stmt.where(PredictionsRegression.prediction_date <= to_date)
        stmt = stmt.order_by(PredictionsRegression.predicted_at.desc()).limit(limit)
        rows = (await db.execute(stmt)).all()
        regression = [_build_regression(r, ticker.symbol) for r in rows]

    if model_type in ("classification", "all"):
        stmt = (
            select(PredictionsClassification, DimDate.trade_date)
            .join(DimDate, PredictionsClassification.date_id == DimDate.date_id)
            .where(PredictionsClassification.ticker_id == ticker.ticker_id)
        )
        if horizon_days:
            stmt = stmt.where(PredictionsClassification.horizon_days == horizon_days)
        if from_date:
            stmt = stmt.where(PredictionsClassification.prediction_date >= from_date)
        if to_date:
            stmt = stmt.where(PredictionsClassification.prediction_date <= to_date)
        stmt = stmt.order_by(PredictionsClassification.predicted_at.desc()).limit(limit)
        rows = (await db.execute(stmt)).all()
        classification = [_build_classification(r, ticker.symbol) for r in rows]

    return PredictionsResponse(symbol=ticker.symbol, regression=regression, classification=classification)


async def get_latest_predictions(db: AsyncSession, symbol: str) -> LatestPrediction | None:
    ticker = await _resolve_ticker(db, symbol)
    if not ticker:
        return None

    reg_map: dict[int, RegressionPrediction] = {}
    for horizon in (1, 5, 20):
        stmt = (
            select(PredictionsRegression, DimDate.trade_date)
            .join(DimDate, PredictionsRegression.date_id == DimDate.date_id)
            .where(
                PredictionsRegression.ticker_id == ticker.ticker_id,
                PredictionsRegression.horizon_days == horizon,
            )
            .order_by(PredictionsRegression.predicted_at.desc())
            .limit(1)
        )
        row = (await db.execute(stmt)).first()
        if row:
            reg_map[horizon] = _build_regression(row, ticker.symbol)

    cls_map: dict[int, ClassificationPrediction] = {}
    for horizon in (1, 5, 20):
        stmt = (
            select(PredictionsClassification, DimDate.trade_date)
            .join(DimDate, PredictionsClassification.date_id == DimDate.date_id)
            .where(
                PredictionsClassification.ticker_id == ticker.ticker_id,
                PredictionsClassification.horizon_days == horizon,
            )
            .order_by(PredictionsClassification.predicted_at.desc())
            .limit(1)
        )
        row = (await db.execute(stmt)).first()
        if row:
            cls_map[horizon] = _build_classification(row, ticker.symbol)

    return LatestPrediction(symbol=ticker.symbol, regression=reg_map, classification=cls_map)


async def list_models(
    db: AsyncSession,
    model_type: str | None,
) -> list[ModelRegistryEntry]:
    stmt = select(ModelRegistry)
    if model_type:
        stmt = stmt.where(ModelRegistry.model_type == model_type)
    stmt = stmt.order_by(ModelRegistry.registered_at.desc())
    rows = (await db.execute(stmt)).scalars().all()
    return [ModelRegistryEntry.model_validate(r) for r in rows]


async def get_model_performance(
    db: AsyncSession,
    model_id: str,
    horizon_days: int | None,
    eval_window_days: int | None,
) -> list[ModelPerformanceSchema]:
    stmt = select(ModelPerformance).where(ModelPerformance.model_id == model_id)
    if horizon_days:
        stmt = stmt.where(ModelPerformance.horizon_days == horizon_days)
    rows = (await db.execute(stmt)).scalars().all()

    perf_ids = [r.perf_id for r in rows]
    cls_map: dict[str, ClassificationModelPerformance] = {}
    reg_map: dict[str, RegressionModelPerformance] = {}

    if perf_ids:
        cls_rows = (
            await db.execute(
                select(ClassificationModelPerformance).where(
                    ClassificationModelPerformance.perf_id.in_(perf_ids)
                )
            )
        ).scalars().all()
        cls_map = {r.perf_id: r for r in cls_rows}

        reg_rows = (
            await db.execute(
                select(RegressionModelPerformance).where(
                    RegressionModelPerformance.perf_id.in_(perf_ids)
                )
            )
        ).scalars().all()
        reg_map = {r.perf_id: r for r in reg_rows}

    result = []
    for perf in rows:
        cls = cls_map.get(perf.perf_id)
        reg = reg_map.get(perf.perf_id)
        ticker_symbol: str | None = None
        if perf.ticker_id:
            t_row = (await db.execute(select(DimTicker).where(DimTicker.ticker_id == perf.ticker_id))).scalar_one_or_none()
            if t_row:
                ticker_symbol = t_row.symbol
        result.append(
            ModelPerformanceSchema(
                perf_id=perf.perf_id,
                model_id=perf.model_id,
                symbol=ticker_symbol,
                eval_date=perf.eval_end_date,
                horizon_days=perf.horizon_days,
                n_samples=perf.n_samples,
                mae=float(reg.mae) if reg and reg.mae is not None else None,
                rmse=float(reg.rmse) if reg and reg.rmse is not None else None,
                accuracy=float(cls.accuracy) if cls and cls.accuracy is not None else None,
                precision_score=float(cls.precision_score) if cls and cls.precision_score is not None else None,
                recall=float(cls.recall) if cls and cls.recall is not None else None,
                f1_score=float(cls.f1_score) if cls and cls.f1_score is not None else None,
                auc_roc=float(cls.auc_roc) if cls and cls.auc_roc is not None else None,
                evaluated_at=perf.evaluated_at,
            )
        )
    return result
