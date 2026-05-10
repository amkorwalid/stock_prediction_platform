"""
train.py
────────────────────────────────────────────────────────────────
Gold → trained models saved to disk + registered in model_registry
       + performance written to model_performance tables

Loads ALL features from both gold tables, trains:
  - XGBClassifier  → direction (1=Up / 0=Down)
  - XGBRegressor   → next-day return → predicted close

Saved artefacts (under models/):
  models/
  ├── cls_h{horizon}_{model_id}.joblib   ← classifier
  ├── reg_h{horizon}_{model_id}.joblib   ← regressor
  └── scaler_h{horizon}_{model_id}.joblib

Usage:
    python train.py --horizon 1
    python train.py --horizon 5  --tickers AAPL,MSFT
    python train.py --horizon 20

Env vars required (in .env):
    ETL_USER_USERNAME, ETL_USER_PASSWORD, DB_HOST, DB_PORT, DB_NAME
"""

import argparse
import logging
import os
import uuid
from datetime import date

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import (accuracy_score, f1_score, log_loss,
                             mean_absolute_error, mean_squared_error,
                             precision_score, recall_score, roc_auc_score)
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text
from xgboost import XGBClassifier, XGBRegressor

load_dotenv()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)
np.random.seed(42)

MODEL_VERSION = "1.0.0"
MODELS_DIR    = "models"

# ── ALL features from both gold tables ───────────────────────────────────────
# feat_price_dynamics
PRICE_FEATURES = [
    "return_1d",
    "lag_1", "lag_2", "lag_3", "lag_7", "lag_20",
    "return_5d", "return_20d", "log_return_1d",
    "ma7", "ma30",
    "momentum_3", "momentum_7",
    "vol_5d", "vol_7d", "vol_20d", "vol_of_vol",
    "price_vs_sma50", "hl_spread", "daily_range", "gap_open",
    "volume_ratio", "rel_volume", "volume_change",
]

# feat_technical
TECH_FEATURES = [
    "sma_5", "sma_10", "sma_20", "sma_50",
    "ema_12", "ema_26",
    "rsi_14",
    "macd", "macd_signal", "macd_hist",
    "boll_upper", "boll_lower", "boll_mid", "atr_14", "bb_position",
    "obv", "vwap",
]

FEATURES = PRICE_FEATURES + TECH_FEATURES


# ── DB ────────────────────────────────────────────────────────────────────────

def get_engine():
    url = (
        f"postgresql://{os.getenv('ETL_USER_USERNAME')}:{os.getenv('ETL_USER_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    )
    return create_engine(url, pool_pre_ping=True)


# ── 1. Load Gold ──────────────────────────────────────────────────────────────

def load_gold(engine, tickers=None):
    sql = """
        SELECT
            dyn.ticker_id,
            dyn.date_id,
            d.trade_date,
            t.symbol,
            f.close,

            -- feat_price_dynamics (all columns)
            dyn.return_1d,
            dyn.lag_1, dyn.lag_2, dyn.lag_3, dyn.lag_7, dyn.lag_20,
            dyn.return_5d, dyn.return_20d, dyn.log_return_1d,
            dyn.ma7, dyn.ma30,
            dyn.momentum_3, dyn.momentum_7,
            dyn.vol_5d, dyn.vol_7d, dyn.vol_20d, dyn.vol_of_vol,
            dyn.price_vs_sma50, dyn.hl_spread, dyn.daily_range, dyn.gap_open,
            dyn.volume_ratio, dyn.rel_volume, dyn.volume_change,

            -- feat_technical (all columns)
            tec.sma_5, tec.sma_10, tec.sma_20, tec.sma_50,
            tec.ema_12, tec.ema_26,
            tec.rsi_14,
            tec.macd, tec.macd_signal, tec.macd_hist,
            tec.boll_upper, tec.boll_lower, tec.boll_mid, tec.atr_14, tec.bb_position,
            tec.obv, tec.vwap

        FROM data_warehouse.feat_price_dynamics  AS dyn
        JOIN data_warehouse.feat_technical        AS tec
            ON tec.ticker_id = dyn.ticker_id AND tec.date_id = dyn.date_id
        JOIN data_warehouse.fact_daily_prices     AS f
            ON f.ticker_id   = dyn.ticker_id AND f.date_id   = dyn.date_id
        JOIN data_warehouse.dim_date              AS d ON d.date_id   = dyn.date_id
        JOIN data_warehouse.dim_ticker            AS t ON t.ticker_id = dyn.ticker_id
    """
    params = {}
    if tickers:
        sql += " WHERE t.symbol = ANY(:t)"
        params["t"] = tickers
    sql += " ORDER BY t.symbol, d.trade_date"

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    log.info("Loaded %d gold rows, %d features", len(df), len(FEATURES))
    return df


# ── 2. Build targets ──────────────────────────────────────────────────────────

def build_targets(df, horizon):
    rows = []
    for _, grp in df.groupby("symbol"):
        g = grp.sort_values("trade_date").copy()
        g["return_next"] = g["return_1d"].shift(-horizon)
        g["price_next"]  = g["close"].shift(-horizon)
        g["target"]      = (g["return_next"] > 0).astype(int)
        rows.append(g)

    out = pd.concat(rows, ignore_index=True)
    out = out.replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=FEATURES + ["return_next", "price_next", "target"])
    return out.reset_index(drop=True)


# ── 3. Temporal 80/20 split ───────────────────────────────────────────────────

def temporal_split(df):
    cut = int(len(df) * 0.8)
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


# ── 4. Train ──────────────────────────────────────────────────────────────────

def train_classifier(X_train, y_train):
    model = XGBClassifier(
        n_estimators=200, max_depth=4, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, eval_metric="logloss", verbosity=0,
    )
    model.fit(X_train, y_train)
    return model


def train_regressor(X_train, y_train_ret):
    model = XGBRegressor(
        n_estimators=300, max_depth=3, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        random_state=42, verbosity=0,
    )
    model.fit(X_train, y_train_ret)
    return model


# ── 5. Evaluate ───────────────────────────────────────────────────────────────

def eval_classifier(model, X_test, y_test):
    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)
    return {
        "accuracy":        float(accuracy_score(y_test, preds)),
        "precision_score": float(precision_score(y_test, preds, zero_division=0)),
        "recall":          float(recall_score(y_test, preds, zero_division=0)),
        "f1_score":        float(f1_score(y_test, preds, zero_division=0)),
        "auc_roc":         float(roc_auc_score(y_test, proba[:, 1])),
        "log_loss":        float(log_loss(y_test, proba)),
    }


def eval_regressor(model, X_test, test_df):
    ret_preds   = model.predict(X_test)
    price_preds = test_df["close"].values * (1 + ret_preds)
    price_true  = test_df["price_next"].values
    return {
        "mae":  float(mean_absolute_error(price_true, price_preds)),
        "rmse": float(np.sqrt(mean_squared_error(price_true, price_preds))),
    }


# ── 6. Save artefacts ─────────────────────────────────────────────────────────

def save_artefacts(cls_model, reg_model, scaler, horizon, model_id):
    os.makedirs(MODELS_DIR, exist_ok=True)
    paths = {
        "cls":    f"{MODELS_DIR}/cls_h{horizon}_{model_id}.joblib",
        "reg":    f"{MODELS_DIR}/reg_h{horizon}_{model_id}.joblib",
        "scaler": f"{MODELS_DIR}/scaler_h{horizon}_{model_id}.joblib",
    }
    joblib.dump(cls_model, paths["cls"])
    joblib.dump(reg_model, paths["reg"])
    joblib.dump(scaler,    paths["scaler"])
    log.info("Saved artefacts → %s", MODELS_DIR)
    return paths


# ── 7. Write to Platinum ──────────────────────────────────────────────────────

def register_model(engine, model_type, horizon, model_id, artefact_path,
                   train_start, train_end):
    model_name = f"xgboost_{model_type}_h{horizon}_v{MODEL_VERSION}"

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE data_warehouse.model_registry
            SET is_champion = false
            WHERE model_type = :mt AND horizon_days = :h AND is_champion = true
        """), {"mt": model_type, "h": horizon})

        conn.execute(text("""
            INSERT INTO data_warehouse.model_registry
                (model_id, model_name, model_type, version, horizon_days,
                 algorithm, feature_set, is_champion,
                 training_start_date, training_end_date, registered_at, notes)
            VALUES
                (:model_id, :model_name, :model_type, :version, :horizon_days,
                 'XGBoost', 'price_only', true,
                 :train_start, :train_end, now(), :notes)
        """), {
            "model_id":    model_id,
            "model_name":  model_name,
            "model_type":  model_type,
            "version":     MODEL_VERSION,
            "horizon_days": horizon,
            "train_start": train_start,
            "train_end":   train_end,
            "notes":       f"artefact={artefact_path} features={len(FEATURES)}",
        })

    log.info("Registered %s  model_id=%s", model_name, model_id)


def insert_performance(engine, model_id, model_type, horizon,
                       n_samples, eval_start, eval_end, metrics):
    perf_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO data_warehouse.model_performance
                (perf_id, model_id, model_type, horizon_days, n_samples,
                 eval_start_date, eval_end_date, evaluated_at)
            VALUES
                (:perf_id, :model_id, :model_type, :horizon_days, :n_samples,
                 :eval_start, :eval_end, now())
        """), {
            "perf_id": perf_id, "model_id": model_id,
            "model_type": model_type, "horizon_days": horizon,
            "n_samples": n_samples,
            "eval_start": eval_start, "eval_end": eval_end,
        })

        if model_type == "classification":
            conn.execute(text("""
                INSERT INTO data_warehouse.classification_model_performance
                    (perf_id, accuracy, precision_score, recall, f1_score, auc_roc, log_loss)
                VALUES (:perf_id, :accuracy, :precision_score, :recall,
                        :f1_score, :auc_roc, :log_loss)
            """), {"perf_id": perf_id, **metrics})
        else:
            conn.execute(text("""
                INSERT INTO data_warehouse.regression_model_performance
                    (perf_id, mae, rmse)
                VALUES (:perf_id, :mae, :rmse)
            """), {"perf_id": perf_id, **metrics})

    log.info("Recorded %s performance: %s", model_type, metrics)


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(horizon, tickers=None):
    engine = get_engine()

    df = load_gold(engine, tickers)
    if df.empty:
        log.info("No gold data — exiting.")
        return

    df = build_targets(df, horizon)
    log.info("After cleaning: %d rows", len(df))

    train_df, test_df = temporal_split(df)
    log.info("Train: %d rows (%s → %s)", len(train_df),
             train_df["trade_date"].min(), train_df["trade_date"].max())
    log.info("Test:  %d rows (%s → %s)", len(test_df),
             test_df["trade_date"].min(), test_df["trade_date"].max())

    # Scale — fit only on train
    scaler     = StandardScaler()
    X_train_sc = scaler.fit_transform(train_df[FEATURES].values)
    X_test_sc  = scaler.transform(test_df[FEATURES].values)

    # Train
    log.info("Training classifier (horizon=%dd)...", horizon)
    cls_model = train_classifier(X_train_sc, train_df["target"].values)

    log.info("Training regressor (horizon=%dd)...", horizon)
    reg_model = train_regressor(X_train_sc, train_df["return_next"].values)

    # Evaluate
    cls_metrics = eval_classifier(cls_model, X_test_sc, test_df["target"].values)
    reg_metrics = eval_regressor(reg_model, X_test_sc, test_df)

    log.info("Classifier — acc=%.4f  f1=%.4f  auc=%.4f",
             cls_metrics["accuracy"], cls_metrics["f1_score"], cls_metrics["auc_roc"])
    log.info("Regressor  — mae=%.4f  rmse=%.4f",
             reg_metrics["mae"], reg_metrics["rmse"])

    # Each model type gets its own UUID (model_registry PK is per row)
    cls_model_id = str(uuid.uuid4())
    reg_model_id = str(uuid.uuid4())

    # Save to disk — artefacts keyed by cls_model_id so predict.py can locate them
    paths = save_artefacts(cls_model, reg_model, scaler, horizon, cls_model_id)

    # Register + performance
    train_start = train_df["trade_date"].min()
    train_end   = train_df["trade_date"].max()
    eval_start  = test_df["trade_date"].min()
    eval_end    = test_df["trade_date"].max()
    n_samples   = len(test_df)

    register_model(engine, "classification", horizon, cls_model_id,
                   paths["cls"], train_start, train_end)
    register_model(engine, "regression",     horizon, reg_model_id,
                   paths["reg"], train_start, train_end)

    insert_performance(engine, cls_model_id, "classification", horizon,
                       n_samples, eval_start, eval_end, cls_metrics)
    insert_performance(engine, reg_model_id, "regression",     horizon,
                       n_samples, eval_start, eval_end, reg_metrics)

    log.info("=== Training complete  horizon=%dd  cls=%s  reg=%s ===",
             horizon, cls_model_id, reg_model_id)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--horizon", type=int, default=1, choices=[1, 5, 20])
    parser.add_argument("--tickers", default=None, help="AAPL,MSFT,...")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    run(args.horizon, tickers)