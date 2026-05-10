from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db_session
from app.schemas import LatestPrediction, ModelPerformance, ModelRegistryEntry, PredictionsResponse
from app.services import PredictionService

router = APIRouter(prefix='/predictions', tags=['predictions'])
DBSession = Annotated[Session, Depends(get_db_session)]


@router.get('/models', response_model=list[ModelRegistryEntry])
def list_models(
    db: DBSession,
    model_type: Literal['regression', 'classification'] | None = None,
) -> list[ModelRegistryEntry]:
    return PredictionService.list_models(db, model_type)


@router.get('/models/{model_id}/performance', response_model=list[ModelPerformance])
def get_model_performance(
    model_id: UUID,
    db: DBSession,
    horizon_days: Literal[1, 5, 20] | None = None,
    eval_window_days: Literal[30, 90, 252] | None = None,
) -> list[ModelPerformance]:
    return PredictionService.model_performance(db, model_id, horizon_days, eval_window_days)


@router.get('/{symbol}/latest', response_model=LatestPrediction)
def get_latest_predictions(symbol: str, db: DBSession) -> LatestPrediction:
    return PredictionService.latest(db, symbol)


@router.get('/{symbol}', response_model=PredictionsResponse)
def get_predictions(
    symbol: str,
    db: DBSession,
    horizon_days: Literal[1, 5, 20] | None = None,
    model_type: Literal['regression', 'classification', 'all'] = 'all',
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    limit: int = Query(default=50, ge=1, le=500),
) -> PredictionsResponse:
    return PredictionService.list_predictions(db, symbol, horizon_days, model_type, date_from, date_to, limit)
