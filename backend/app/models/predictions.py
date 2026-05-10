from datetime import date, datetime
from uuid import UUID

from sqlalchemy import Boolean, Date, DateTime, Integer, Numeric, SmallInteger, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ModelRegistry(Base):
    __tablename__ = "model_registry"
    __table_args__ = {"schema": "data_warehouse"}

    model_id: Mapped[str] = mapped_column(String, primary_key=True)
    model_name: Mapped[str] = mapped_column(String, nullable=False)
    model_type: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)
    algorithm: Mapped[str | None] = mapped_column(String)
    feature_set: Mapped[str | None] = mapped_column(String)
    training_start_date: Mapped[date | None] = mapped_column(Date)
    training_end_date: Mapped[date | None] = mapped_column(Date)
    val_metric_name: Mapped[str | None] = mapped_column(String)
    val_metric_value: Mapped[float | None] = mapped_column(Numeric(12, 6))
    is_champion: Mapped[bool] = mapped_column(Boolean, default=False)
    registered_at: Mapped[datetime | None] = mapped_column(DateTime)
    notes: Mapped[str | None] = mapped_column(Text)


class PredictionsRegression(Base):
    __tablename__ = "predictions_regression"
    __table_args__ = (
        UniqueConstraint("ticker_id", "date_id", "horizon_days", "model_id", name="uq_pred_reg"),
        {"schema": "data_warehouse"},
    )

    pred_id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)

    pred_close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    pred_return: Mapped[float | None] = mapped_column(Numeric(10, 6))
    pred_log_return: Mapped[float | None] = mapped_column(Numeric(10, 6))
    confidence_lo: Mapped[float | None] = mapped_column(Numeric(12, 4))
    confidence_hi: Mapped[float | None] = mapped_column(Numeric(12, 4))
    confidence_level: Mapped[float | None] = mapped_column(Numeric(5, 2), default=0.95)

    actual_close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    actual_return: Mapped[float | None] = mapped_column(Numeric(10, 6))
    mae: Mapped[float | None] = mapped_column(Numeric(10, 6))
    rmse: Mapped[float | None] = mapped_column(Numeric(10, 6))

    predicted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    prediction_date: Mapped[date] = mapped_column(Date, nullable=False)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_version: Mapped[str | None] = mapped_column(String)


class PredictionsClassification(Base):
    __tablename__ = "predictions_classification"
    __table_args__ = (
        UniqueConstraint("ticker_id", "date_id", "horizon_days", "model_id", name="uq_pred_cls"),
        {"schema": "data_warehouse"},
    )

    pred_id: Mapped[str] = mapped_column(String, primary_key=True)
    ticker_id: Mapped[int] = mapped_column(Integer, nullable=False)
    date_id: Mapped[int] = mapped_column(Integer, nullable=False)
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)

    pred_direction: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    prob_up: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    prob_flat: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    prob_down: Mapped[float] = mapped_column(Numeric(6, 4), nullable=False)
    pred_confidence: Mapped[float | None] = mapped_column(Numeric(6, 4))
    is_high_confidence: Mapped[bool | None] = mapped_column(Boolean)

    actual_direction: Mapped[int | None] = mapped_column(SmallInteger)
    is_correct: Mapped[bool | None] = mapped_column(Boolean)

    predicted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    prediction_date: Mapped[date] = mapped_column(Date, nullable=False)
    target_date: Mapped[date] = mapped_column(Date, nullable=False)
    data_version: Mapped[str | None] = mapped_column(String)


class ModelPerformance(Base):
    __tablename__ = "model_performance"
    __table_args__ = {"schema": "data_warehouse"}

    perf_id: Mapped[str] = mapped_column(String, primary_key=True)
    model_id: Mapped[str] = mapped_column(String, nullable=False)
    ticker_id: Mapped[int | None] = mapped_column(Integer)
    model_type: Mapped[str] = mapped_column(String, nullable=False)
    eval_start_date: Mapped[date | None] = mapped_column(Date)
    eval_end_date: Mapped[date | None] = mapped_column(Date)
    horizon_days: Mapped[int] = mapped_column(Integer, nullable=False)
    n_samples: Mapped[int] = mapped_column(Integer, nullable=False)
    evaluated_at: Mapped[datetime | None] = mapped_column(DateTime)


class ClassificationModelPerformance(Base):
    __tablename__ = "classification_model_performance"
    __table_args__ = {"schema": "data_warehouse"}

    perf_id: Mapped[str] = mapped_column(String, primary_key=True)
    accuracy: Mapped[float | None] = mapped_column(Numeric(6, 4))
    precision_score: Mapped[float | None] = mapped_column(Numeric(6, 4))
    recall: Mapped[float | None] = mapped_column(Numeric(6, 4))
    f1_score: Mapped[float | None] = mapped_column(Numeric(6, 4))
    auc_roc: Mapped[float | None] = mapped_column(Numeric(6, 4))
    log_loss: Mapped[float | None] = mapped_column(Numeric(10, 6))


class RegressionModelPerformance(Base):
    __tablename__ = "regression_model_performance"
    __table_args__ = {"schema": "data_warehouse"}

    perf_id: Mapped[str] = mapped_column(String, primary_key=True)
    mae: Mapped[float | None] = mapped_column(Numeric(10, 6))
    rmse: Mapped[float | None] = mapped_column(Numeric(10, 6))
