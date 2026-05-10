from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class RegressionPrediction(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    pred_id: str
    symbol: str
    prediction_date: date
    target_date: date
    horizon_days: int
    model_id: str
    pred_close: float | None = None
    pred_return: float | None = None
    pred_log_return: float | None = None
    confidence_lo: float | None = None
    confidence_hi: float | None = None
    actual_close: float | None = None
    actual_return: float | None = None
    mae: float | None = None
    predicted_at: datetime


class ClassificationPrediction(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    pred_id: str
    symbol: str
    prediction_date: date
    target_date: date
    horizon_days: int
    model_id: str
    pred_direction: int
    prob_up: float
    prob_flat: float
    prob_down: float
    pred_confidence: float | None = None
    is_high_confidence: bool | None = None
    actual_direction: int | None = None
    is_correct: bool | None = None
    predicted_at: datetime


class PredictionsResponse(BaseModel):
    symbol: str
    regression: list[RegressionPrediction]
    classification: list[ClassificationPrediction]


class LatestPrediction(BaseModel):
    symbol: str
    regression: dict[int, RegressionPrediction]
    classification: dict[int, ClassificationPrediction]


class ModelRegistryEntry(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    model_id: str
    model_name: str
    model_type: Literal["regression", "classification"]
    version: str
    horizon_days: int
    algorithm: str | None = None
    feature_set: str | None = None
    training_start_date: date | None = None
    training_end_date: date | None = None
    val_metric_name: str | None = None
    val_metric_value: float | None = None
    is_champion: bool
    registered_at: datetime | None = None


class ModelPerformance(BaseModel):
    perf_id: str
    model_id: str
    symbol: str | None = None
    eval_date: date | None = None
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
    drift_flag: bool | None = None
    drift_severity: Literal["none", "warning", "critical"] | None = None
    evaluated_at: datetime | None = None
