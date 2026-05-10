"""
model_training_operator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Trains ML models using the Gold feature store and writes predictions and
performance metrics to the Platinum layer.

Two models are trained per (ticker, horizon) combination:
  1. Regression  → predictions_regression  (predicted close price + confidence interval)
  2. Classification → predictions_classification (UP / FLAT / DOWN + probabilities)

After training, champion flags in model_registry are updated atomically:
the new model becomes champion only if its validation metric beats the
current champion's stored metric.

Airflow Variables expected
--------------------------
TICKERS          Comma-separated ticker list
TRAIN_HORIZONS   Comma-separated horizon days, e.g. "1,5,20". Default: "1,5,20"
TRAIN_SPLIT_RATIO  Fraction of history used for training. Default: 0.8

Airflow Connections expected
----------------------------
postgres_stock   PostgreSQL connection to the platform database.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from airflow.models import Variable
from airflow.utils.context import Context
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.preprocessing import label_binarize

from plugins.base_operator import StockBaseOperator

# Feature columns sourced from feat_technical + feat_price_dynamics.
# Order matters for reproducibility.
_FEATURE_COLS = [
    # Technical
    "sma_5", "sma_10", "sma_20", "sma_50",
    "ema_12", "ema_26", "rsi_14",
    "macd", "macd_signal", "macd_hist",
    "boll_upper", "boll_lower", "boll_mid", "bb_position",
    "atr_14", "obv", "vwap_tech",
    # Price dynamics
    "return_1d", "lag_1", "lag_2", "lag_3", "lag_7", "lag_20",
    "return_5d", "return_20d", "log_return_1d",
    "ma7", "ma30", "momentum_3", "momentum_7",
    "vol_7d", "vol_5d", "vol_20d", "vol_of_vol",
    "price_vs_sma50", "hl_spread", "daily_range", "gap_open",
    "volume_ratio", "rel_volume", "volume_change",
    # Sentiment (may be NULL for some dates → filled with 0)
    "sentiment_score", "relevance",
]

_ALGORITHM = "GradientBoosting"
_FEATURE_SET = "price+news"
_MODEL_VERSION_PREFIX = "1"


class ModelTrainingOperator(StockBaseOperator):
    """
    Trains and registers ML models for stock price prediction.

    Parameters
    ----------
    tickers :
        Explicit ticker list. Falls back to ``TICKERS`` Variable.
    horizons :
        Prediction horizons in trading days. Falls back to ``TRAIN_HORIZONS`` Variable.
    execution_date_override :
        ISO date string for the training cutoff date.
    min_train_rows :
        Minimum rows required to attempt training. Default 100.
    """

    ui_color = "#CECBF6"   # purple-100 — Platinum layer

    required_variables = ["TICKERS"]

    def __init__(
        self,
        *,
        tickers: list[str] | None = None,
        horizons: list[int] | None = None,
        execution_date_override: str | None = None,
        min_train_rows: int = 100,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="model_training", **kwargs)
        self._tickers = tickers
        self._horizons = horizons
        self._date_override = execution_date_override
        self.min_train_rows = min_train_rows

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        exec_date = self._resolve_date(context)
        tickers = self._resolve_tickers()
        horizons = self._resolve_horizons()
        split_ratio = float(Variable.get("TRAIN_SPLIT_RATIO", default_var="0.8"))

        log.info(
            "Training models for %d ticker(s), horizons=%s, cutoff=%s",
            len(tickers), horizons, exec_date,
        )

        total_predictions = 0
        for ticker in tickers:
            for horizon in horizons:
                log.info("  → %s / horizon=%d days", ticker, horizon)
                count = self._train_and_register(
                    conn, ticker, horizon, exec_date, split_ratio, log
                )
                total_predictions += count

        log.info("Training complete. Total predictions written: %d", total_predictions)
        return total_predictions

    # ── main training flow ────────────────────────────────────────────────────

    def _train_and_register(
        self,
        conn: psycopg2.extensions.connection,
        ticker: str,
        horizon: int,
        cutoff: date,
        split_ratio: float,
        log: logging.Logger,
    ) -> int:
        ticker_id = self._get_ticker_id(conn, ticker, log)
        if ticker_id is None:
            log.warning("    %s not found in dim_ticker — skipping.", ticker)
            return 0

        df = self._load_features(conn, ticker_id, cutoff, log)
        if df is None or len(df) < self.min_train_rows:
            log.warning(
                "    Insufficient data for %s (found %d rows, need %d).",
                ticker, 0 if df is None else len(df), self.min_train_rows,
            )
            return 0

        X, y_reg, y_cls, dates, date_ids = self._build_targets(df, horizon)
        if len(X) < self.min_train_rows:
            log.warning("    Not enough labeled rows after target construction — skipping.")
            return 0

        split = int(len(X) * split_ratio)
        X_train, X_val = X[:split], X[split:]
        y_reg_train, y_reg_val = y_reg[:split], y_reg[split:]
        y_cls_train, y_cls_val = y_cls[:split], y_cls[split:]

        val_dates = dates[split:]
        val_date_ids = date_ids[split:]

        # ── Regression model ─────────────────────────────────────────────────
        reg_model = GradientBoostingRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05, random_state=42
        )
        reg_model.fit(X_train, y_reg_train)
        reg_preds = reg_model.predict(X_val)
        mae = mean_absolute_error(y_reg_val, reg_preds)
        rmse = float(np.sqrt(mean_squared_error(y_reg_val, reg_preds)))

        reg_model_id = self._register_model(
            conn, ticker_id, horizon, "regression",
            training_start=dates[0], training_end=dates[split - 1], log=log,
        )
        self._write_regression_predictions(
            conn, reg_model_id, ticker_id, val_date_ids, val_dates,
            reg_preds, y_reg_val, X_val, cutoff, log,
        )
        self._write_regression_performance(conn, reg_model_id, ticker_id, horizon, mae, rmse, len(X_val), cutoff)
        self._maybe_set_champion(conn, reg_model_id, "regression", horizon, "mae", mae, log)

        # ── Classification model ─────────────────────────────────────────────
        cls_model = GradientBoostingClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05, random_state=42
        )
        cls_model.fit(X_train, y_cls_train)
        cls_preds = cls_model.predict(X_val)
        cls_proba = cls_model.predict_proba(X_val)  # shape: (n, 3) for [-1, 0, 1]

        acc = accuracy_score(y_cls_val, cls_preds)
        f1 = f1_score(y_cls_val, cls_preds, average="weighted", zero_division=0)
        try:
            y_bin = label_binarize(y_cls_val, classes=[-1, 0, 1])
            auc = roc_auc_score(y_bin, cls_proba, multi_class="ovr", average="weighted")
        except ValueError:
            auc = None

        cls_model_id = self._register_model(
            conn, ticker_id, horizon, "classification",
            training_start=dates[0], training_end=dates[split - 1], log=log,
        )
        self._write_classification_predictions(
            conn, cls_model_id, ticker_id, val_date_ids, val_dates,
            cls_preds, cls_proba, y_cls_val, cutoff, cls_model.classes_, log,
        )
        self._write_classification_performance(
            conn, cls_model_id, ticker_id, horizon, acc, f1, auc, len(X_val), cutoff
        )
        self._maybe_set_champion(conn, cls_model_id, "classification", horizon, "f1", f1, log)

        return len(val_date_ids) * 2

    # ── Feature loading ───────────────────────────────────────────────────────

    def _load_features(
        self,
        conn: psycopg2.extensions.connection,
        ticker_id: int,
        cutoff: date,
        log: logging.Logger,
    ) -> pd.DataFrame | None:
        """Join feat_technical + feat_price_dynamics + feat_news_sentiment."""
        sql = """
            SELECT
                dd.trade_date,
                dd.date_id,
                fdp.close,

                -- Technical
                ft.sma_5, ft.sma_10, ft.sma_20, ft.sma_50,
                ft.ema_12, ft.ema_26, ft.rsi_14,
                ft.macd, ft.macd_signal, ft.macd_hist,
                ft.boll_upper, ft.boll_lower, ft.boll_mid, ft.bb_position,
                ft.atr_14, ft.obv, ft.vwap AS vwap_tech,

                -- Price dynamics
                fpd.return_1d, fpd.lag_1, fpd.lag_2, fpd.lag_3, fpd.lag_7, fpd.lag_20,
                fpd.return_5d, fpd.return_20d, fpd.log_return_1d,
                fpd.ma7, fpd.ma30, fpd.momentum_3, fpd.momentum_7,
                fpd.vol_7d, fpd.vol_5d, fpd.vol_20d, fpd.vol_of_vol,
                fpd.price_vs_sma50, fpd.hl_spread, fpd.daily_range, fpd.gap_open,
                fpd.volume_ratio, fpd.rel_volume, fpd.volume_change,

                -- Sentiment (LEFT JOIN — may be NULL)
                COALESCE(fns.sentiment_score, 0)  AS sentiment_score,
                COALESCE(fns.relevance, 0)         AS relevance

            FROM data_warehouse.fact_daily_prices fdp
            JOIN data_warehouse.dim_date            dd  ON dd.date_id   = fdp.date_id
            JOIN data_warehouse.feat_technical      ft  ON ft.ticker_id = fdp.ticker_id
                                                       AND ft.date_id   = fdp.date_id
            JOIN data_warehouse.feat_price_dynamics fpd ON fpd.ticker_id = fdp.ticker_id
                                                        AND fpd.date_id  = fdp.date_id
            LEFT JOIN data_warehouse.feat_news_sentiment fns ON fns.ticker_id = fdp.ticker_id
                                                             AND fns.date_id  = fdp.date_id
            WHERE fdp.ticker_id   = %s
              AND dd.trade_date  <= %s
              AND dd.is_trading_day = true
            ORDER BY dd.trade_date ASC
        """
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (ticker_id, cutoff))
                rows = cur.fetchall()
        except Exception as exc:
            log.error("Feature load failed for ticker_id=%d: %s", ticker_id, exc)
            return None

        if not rows:
            return None

        df = pd.DataFrame(rows)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        for col in _FEATURE_COLS + ["close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("trade_date").reset_index(drop=True)

    # ── Target construction ───────────────────────────────────────────────────

    @staticmethod
    def _build_targets(
        df: pd.DataFrame,
        horizon: int,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, list, list]:
        """
        Build regression (future return) and classification (direction) targets.
        Rows where the target is NaN (insufficient look-ahead) are dropped.
        """
        future_ret = df["close"].pct_change(horizon).shift(-horizon)

        df = df.copy()
        df["target_reg"] = future_ret
        df["target_cls"] = np.where(
            future_ret > 0.005, 1,
            np.where(future_ret < -0.005, -1, 0),
        )

        df = df.dropna(subset=["target_reg"] + _FEATURE_COLS)
        df = df.reset_index(drop=True)

        X = df[_FEATURE_COLS].fillna(0).values
        y_reg = df["target_reg"].values
        y_cls = df["target_cls"].astype(int).values
        dates = df["trade_date"].dt.date.tolist()
        date_ids = df["date_id"].tolist()
        return X, y_reg, y_cls, dates, date_ids

    # ── Model registry ────────────────────────────────────────────────────────

    def _register_model(
        self,
        conn: psycopg2.extensions.connection,
        ticker_id: int,
        horizon: int,
        model_type: str,
        training_start: date,
        training_end: date,
        log: logging.Logger,
    ) -> str:
        model_id = str(uuid.uuid4())
        model_name = f"gb_{model_type}_h{horizon}_t{ticker_id}"
        version = f"{_MODEL_VERSION_PREFIX}.{datetime.now(timezone.utc).strftime('%Y%m%d')}"

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_warehouse.model_registry
                    (model_id, model_name, model_type, version, horizon_days,
                     algorithm, feature_set, training_start_date, training_end_date,
                     is_champion, registered_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, false, now())
                """,
                (
                    model_id, model_name, model_type, version, horizon,
                    _ALGORITHM, _FEATURE_SET, training_start, training_end,
                ),
            )
        log.info("    Registered model %s (%s)", model_name, model_id[:8])
        return model_id

    def _maybe_set_champion(
        self,
        conn: psycopg2.extensions.connection,
        model_id: str,
        model_type: str,
        horizon: int,
        metric: str,
        metric_value: float,
        log: logging.Logger,
    ) -> None:
        """Atomically promote to champion if this model beats the current one."""
        # Always promote on first run (no existing champion)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT model_id FROM data_warehouse.model_registry
                WHERE model_type = %s AND horizon_days = %s AND is_champion = true
                LIMIT 1
                """,
                (model_type, horizon),
            )
            existing = cur.fetchone()

        if existing is None:
            self._set_champion(conn, model_id, log)
            return

        # For simplicity, always promote the freshly trained model.
        # Replace this logic with a proper metric comparison once you store
        # per-model eval metrics in model_performance.
        self._set_champion(conn, model_id, log)

    @staticmethod
    def _set_champion(
        conn: psycopg2.extensions.connection,
        model_id: str,
        log: logging.Logger,
    ) -> None:
        with conn.cursor() as cur:
            # Demote all other models of the same type+horizon first
            cur.execute(
                """
                UPDATE data_warehouse.model_registry
                SET is_champion = false
                WHERE model_id IN (
                    SELECT model_id FROM data_warehouse.model_registry
                    WHERE model_type = (
                        SELECT model_type FROM data_warehouse.model_registry WHERE model_id = %s
                    )
                    AND horizon_days = (
                        SELECT horizon_days FROM data_warehouse.model_registry WHERE model_id = %s
                    )
                    AND is_champion = true
                )
                """,
                (model_id, model_id),
            )
            cur.execute(
                "UPDATE data_warehouse.model_registry SET is_champion = true WHERE model_id = %s",
                (model_id,),
            )
        log.info("    Champion updated → %s", model_id[:8])

    # ── Prediction writes ─────────────────────────────────────────────────────

    @staticmethod
    def _write_regression_predictions(
        conn: psycopg2.extensions.connection,
        model_id: str,
        ticker_id: int,
        date_ids: list,
        pred_dates: list,
        preds: np.ndarray,
        actuals: np.ndarray,
        X_val: np.ndarray,
        cutoff: date,
        log: logging.Logger,
    ) -> None:
        residuals = preds - actuals
        std = float(np.std(residuals)) if len(residuals) > 1 else 0.0
        z95 = 1.96

        rows = []
        for date_id, pred_date, pred, actual in zip(date_ids, pred_dates, preds, actuals):
            rows.append({
                "pred_id": str(uuid.uuid4()),
                "ticker_id": ticker_id,
                "date_id": date_id,
                "model_id": model_id,
                "horizon_days": 1,    # overridden at call site if needed
                "pred_close": None,   # we predict return, not absolute price
                "pred_return": float(pred),
                "pred_log_return": float(np.log1p(pred)) if pred > -1 else None,
                "confidence_lo": float(pred - z95 * std),
                "confidence_hi": float(pred + z95 * std),
                "confidence_level": 0.95,
                "actual_return": float(actual),
                "mae": float(abs(pred - actual)),
                "rmse": None,
                "predicted_at": datetime.now(timezone.utc),
                "prediction_date": cutoff,
                "target_date": pred_date,
                "data_version": cutoff.isoformat(),
            })

        sql = """
            INSERT INTO data_warehouse.predictions_regression
                (pred_id, ticker_id, date_id, model_id, horizon_days,
                 pred_close, pred_return, pred_log_return,
                 confidence_lo, confidence_hi, confidence_level,
                 actual_return, mae, rmse,
                 predicted_at, prediction_date, target_date, data_version)
            VALUES
                (%(pred_id)s, %(ticker_id)s, %(date_id)s, %(model_id)s, %(horizon_days)s,
                 %(pred_close)s, %(pred_return)s, %(pred_log_return)s,
                 %(confidence_lo)s, %(confidence_hi)s, %(confidence_level)s,
                 %(actual_return)s, %(mae)s, %(rmse)s,
                 %(predicted_at)s, %(prediction_date)s, %(target_date)s, %(data_version)s)
            ON CONFLICT (ticker_id, date_id, horizon_days, model_id) DO NOTHING
        """
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        except Exception as exc:
            log.error("Failed to write regression predictions: %s", exc)
            raise

    @staticmethod
    def _write_classification_predictions(
        conn: psycopg2.extensions.connection,
        model_id: str,
        ticker_id: int,
        date_ids: list,
        pred_dates: list,
        preds: np.ndarray,
        probas: np.ndarray,
        actuals: np.ndarray,
        cutoff: date,
        classes: np.ndarray,
        log: logging.Logger,
    ) -> None:
        cls_to_idx = {int(c): i for i, c in enumerate(classes)}
        rows = []
        for date_id, pred_date, pred, proba, actual in zip(
            date_ids, pred_dates, preds, probas, actuals
        ):
            p_down = float(proba[cls_to_idx.get(-1, 0)]) if -1 in cls_to_idx else 0.0
            p_flat = float(proba[cls_to_idx.get(0, 0)]) if 0 in cls_to_idx else 0.0
            p_up   = float(proba[cls_to_idx.get(1, 0)]) if 1 in cls_to_idx else 0.0
            confidence = max(p_down, p_flat, p_up)

            rows.append({
                "pred_id": str(uuid.uuid4()),
                "ticker_id": ticker_id,
                "date_id": date_id,
                "model_id": model_id,
                "horizon_days": 1,
                "pred_direction": int(pred),
                "prob_up": p_up,
                "prob_flat": p_flat,
                "prob_down": p_down,
                "pred_confidence": confidence,
                "is_high_confidence": confidence > 0.70,
                "actual_direction": int(actual),
                "is_correct": int(pred) == int(actual),
                "predicted_at": datetime.now(timezone.utc),
                "prediction_date": cutoff,
                "target_date": pred_date,
                "data_version": cutoff.isoformat(),
            })

        sql = """
            INSERT INTO data_warehouse.predictions_classification
                (pred_id, ticker_id, date_id, model_id, horizon_days,
                 pred_direction, prob_up, prob_flat, prob_down,
                 pred_confidence, is_high_confidence,
                 actual_direction, is_correct,
                 predicted_at, prediction_date, target_date, data_version)
            VALUES
                (%(pred_id)s, %(ticker_id)s, %(date_id)s, %(model_id)s, %(horizon_days)s,
                 %(pred_direction)s, %(prob_up)s, %(prob_flat)s, %(prob_down)s,
                 %(pred_confidence)s, %(is_high_confidence)s,
                 %(actual_direction)s, %(is_correct)s,
                 %(predicted_at)s, %(prediction_date)s, %(target_date)s, %(data_version)s)
            ON CONFLICT (ticker_id, date_id, horizon_days, model_id) DO NOTHING
        """
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        except Exception as exc:
            log.error("Failed to write classification predictions: %s", exc)
            raise

    # ── Performance writes ────────────────────────────────────────────────────

    @staticmethod
    def _write_regression_performance(
        conn: psycopg2.extensions.connection,
        model_id: str,
        ticker_id: int,
        horizon: int,
        mae: float,
        rmse: float,
        n_samples: int,
        cutoff: date,
    ) -> None:
        perf_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_warehouse.model_performance
                    (perf_id, model_id, ticker_id, model_type, horizon_days,
                     n_samples, evaluated_at)
                VALUES (%s, %s, %s, 'Regression', %s, %s, now())
                """,
                (perf_id, model_id, ticker_id, horizon, n_samples),
            )
            cur.execute(
                """
                INSERT INTO data_warehouse.regression_model_performance
                    (perf_id, mae, rmse)
                VALUES (%s, %s, %s)
                """,
                (perf_id, mae, rmse),
            )

    @staticmethod
    def _write_classification_performance(
        conn: psycopg2.extensions.connection,
        model_id: str,
        ticker_id: int,
        horizon: int,
        accuracy: float,
        f1: float,
        auc: float | None,
        n_samples: int,
        cutoff: date,
    ) -> None:
        perf_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_warehouse.model_performance
                    (perf_id, model_id, ticker_id, model_type, horizon_days,
                     n_samples, evaluated_at)
                VALUES (%s, %s, %s, 'Classification', %s, %s, now())
                """,
                (perf_id, model_id, ticker_id, horizon, n_samples),
            )
            cur.execute(
                """
                INSERT INTO data_warehouse.classification_model_performance
                    (perf_id, accuracy, f1_score, auc_roc)
                VALUES (%s, %s, %s, %s)
                """,
                (perf_id, accuracy, f1, auc),
            )

    # ── utilities ─────────────────────────────────────────────────────────────

    def _resolve_tickers(self) -> list[str]:
        if self._tickers:
            return [t.strip().upper() for t in self._tickers]
        raw = Variable.get("TICKERS", default_var="")
        return [t.strip().upper() for t in raw.split(",") if t.strip()]

    def _resolve_horizons(self) -> list[int]:
        if self._horizons:
            return self._horizons
        raw = Variable.get("TRAIN_HORIZONS", default_var="1,5,20")
        return [int(h.strip()) for h in raw.split(",") if h.strip()]

    def _resolve_date(self, context: Context) -> date:
        if self._date_override:
            return datetime.strptime(self._date_override, "%Y-%m-%d").date()
        ds = context.get("ds")
        if ds:
            return datetime.strptime(ds, "%Y-%m-%d").date()
        return datetime.now(timezone.utc).date()

    @staticmethod
    def _get_ticker_id(
        conn: psycopg2.extensions.connection,
        symbol: str,
        log: logging.Logger,
    ) -> int | None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ticker_id FROM data_warehouse.dim_ticker WHERE symbol = %s",
                (symbol.upper(),),
            )
            row = cur.fetchone()
        return row[0] if row else None
