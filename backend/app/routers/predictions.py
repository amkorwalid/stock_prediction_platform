from datetime import date
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas.predictions import (
    LatestPrediction,
    ModelPerformance,
    ModelRegistryEntry,
    PredictionsResponse,
)
from app.services import prediction_service

router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/models", response_model=list[ModelRegistryEntry])
async def list_models(
    model_type: Literal["regression", "classification"] | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await prediction_service.list_models(db, model_type)


@router.get("/models/{model_id}/performance", response_model=list[ModelPerformance])
async def get_model_performance(
    model_id: str,
    horizon_days: Literal[1, 5, 20] | None = Query(None),
    eval_window_days: Literal[30, 90, 252] | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await prediction_service.get_model_performance(db, model_id, horizon_days, eval_window_days)


@router.get("/{symbol}/latest", response_model=LatestPrediction)
async def get_latest_predictions(symbol: str, db: AsyncSession = Depends(get_db)):
    result = await prediction_service.get_latest_predictions(db, symbol)
    if not result:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return result


@router.get("/{symbol}", response_model=PredictionsResponse)
async def get_predictions(
    symbol: str,
    horizon_days: Literal[1, 5, 20] | None = Query(None),
    model_type: Literal["regression", "classification", "all"] = Query("all"),
    from_: date | None = Query(None, alias="from"),
    to: date | None = Query(None),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
):
    result = await prediction_service.get_predictions(
        db, symbol, horizon_days, model_type, from_, to, limit
    )
    if not result:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return result
