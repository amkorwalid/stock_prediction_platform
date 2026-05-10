"""
predict.py
────────────────────────────────────────────────────────────────
Loads champion models from disk and writes predictions to Platinum:
  → predictions_regression
  → predictions_classification

Does NOT retrain. Reads the champion model_id from model_registry,
loads the matching .joblib artefacts, runs inference on the latest
gold features, and inserts prediction rows.

Usage:
    python predict.py --date 2026-05-08
    python predict.py --date 2026-05-08 --horizon 5
    python predict.py --date 2026-05-08 --horizon 1 --tickers AAPL,MSFT

Env vars required (in .env):
    ETL_USER_USERNAME, ETL_USER_PASSWORD, DB_HOST, DB_PORT, DB_NAME
"""

import argparse
import logging
import os
from datetime import date, datetime, timedelta

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

MODELS_DIR = "models"

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


# ── 1. Resolve champion model ─────────────────────────────────────────────────

def get_champion(engine, model_type, horizon):
    """
    Returns (model_id, artefact_path) for the current champion,
    or raises if none is registered.
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT model_id, notes
            FROM data_warehouse.model_registry
            WHERE model_type  = :mt
              AND horizon_days = :h
              AND is_champion  = true
            ORDER BY registered_at DESC
            LIMIT 1
        """), {"mt": model_type, "h": horizon}).fetchone()

    if not row:
        raise RuntimeError(
            f"No champion {model_type} model for horizon={horizon}. Run train.py first."
        )

    model_id = str(row[0])
    # notes column stores "artefact=models/cls_h1_<uuid>.joblib features=N"
    notes = row[1] or ""
    path  = next((p.split("=")[1] for p in notes.split() if p.startswith("artefact=")), None)
    return model_id, path


# ── 2. Load artefacts from disk ───────────────────────────────────────────────

def load_artefacts(horizon, model_id):
    cls_path    = f"{MODELS_DIR}/cls_h{horizon}_{model_id}.joblib"
    reg_path    = f"{MODELS_DIR}/reg_h{horizon}_{model_id}.joblib"
    scaler_path = f"{MODELS_DIR}/scaler_h{horizon}_{model_id}.joblib"

    for p in [cls_path, reg_path, scaler_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(
                f"Artefact not found: {p}\n"
                f"Run: python train.py --horizon {horizon}"
            )

    cls_model = joblib.load(cls_path)
    reg_model = joblib.load(reg_path)
    scaler    = joblib.load(scaler_path)
    log.info("Loaded artefacts for model_id=%s  horizon=%d", model_id, horizon)
    return cls_model, reg_model, scaler


# ── 3. Load features for prediction date ─────────────────────────────────────

def load_features(engine, prediction_date, tickers=None):
    """
    Loads the gold feature row for exactly the prediction date.
    Also pulls `close` from fact_daily_prices for price projection.
    """
    sql = """
        SELECT
            dyn.ticker_id,
            dyn.date_id,
            d.trade_date,
            t.symbol,
            f.close,

            dyn.return_1d,
            dyn.lag_1, dyn.lag_2, dyn.lag_3, dyn.lag_7, dyn.lag_20,
            dyn.return_5d, dyn.return_20d, dyn.log_return_1d,
            dyn.ma7, dyn.ma30,
            dyn.momentum_3, dyn.momentum_7,
            dyn.vol_5d, dyn.vol_7d, dyn.vol_20d, dyn.vol_of_vol,
            dyn.price_vs_sma50, dyn.hl_spread, dyn.daily_range, dyn.gap_open,
            dyn.volume_ratio, dyn.rel_volume, dyn.volume_change,

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
        WHERE d.trade_date = :pred_date
    """
    params: dict = {"pred_date": prediction_date}
    if tickers:
        sql += " AND t.symbol = ANY(:t)"
        params["t"] = tickers

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    log.info("Loaded %d feature rows for %s", len(df), prediction_date)
    return df


# ── 4. Run inference ──────────────────────────────────────────────────────────

def predict(df, cls_model, reg_model, scaler):
    X = df[FEATURES].replace([np.inf, -np.inf], np.nan)

    # Rows with any NaN feature cannot be predicted
    valid_mask = X.notna().all(axis=1)
    n_invalid = (~valid_mask).sum()
    if n_invalid:
        log.warning("%d rows skipped due to NaN features (insufficient history?)", n_invalid)

    X_sc = scaler.transform(X[valid_mask].values)

    cls_preds = cls_model.predict(X_sc)
    cls_proba = cls_model.predict_proba(X_sc)   # [prob_down, prob_up]
    ret_preds = reg_model.predict(X_sc)

    return df[valid_mask].copy(), cls_preds, cls_proba, ret_preds


# ── 5. Write predictions ──────────────────────────────────────────────────────

def get_date_id(conn, trade_date):
    row = conn.execute(
        text("SELECT date_id FROM data_warehouse.dim_date WHERE trade_date = :d"),
        {"d": trade_date}
    ).fetchone()
    return row[0] if row else None


def insert_regression_predictions(engine, valid_df, ret_preds, cls_model_id,
                                   reg_model_id, horizon, prediction_date):
    sql = text("""
        INSERT INTO data_warehouse.predictions_regression
            (ticker_id, date_id, model_id, horizon_days,
             pred_close, pred_return, confidence_level,
             predicted_at, prediction_date, target_date)
        VALUES
            (:ticker_id, :date_id, :model_id, :horizon_days,
             :pred_close, :pred_return, 0.95,
             now(), :prediction_date, :target_date)
        ON CONFLICT (ticker_id, date_id, horizon_days, model_id) DO NOTHING
    """)
    with engine.begin() as conn:
        for i, (_, row) in enumerate(valid_df.iterrows()):
            date_id = get_date_id(conn, row["trade_date"])
            if not date_id:
                continue
            pred_return = float(ret_preds[i])
            conn.execute(sql, {
                "ticker_id":       int(row["ticker_id"]),
                "date_id":         date_id,
                "model_id":        reg_model_id,
                "horizon_days":    horizon,
                "pred_close":      round(float(row["close"]) * (1 + pred_return), 4),
                "pred_return":     round(pred_return, 6),
                "prediction_date": prediction_date,
                "target_date":     prediction_date + timedelta(days=horizon),
            })
    log.info("Inserted %d regression predictions", len(valid_df))


def insert_classification_predictions(engine, valid_df, cls_preds, cls_proba,
                                       cls_model_id, horizon, prediction_date):
    sql = text("""
        INSERT INTO data_warehouse.predictions_classification
            (ticker_id, date_id, model_id, horizon_days,
             pred_direction, prob_up, prob_flat, prob_down,
             pred_confidence, is_high_confidence,
             predicted_at, prediction_date, target_date)
        VALUES
            (:ticker_id, :date_id, :model_id, :horizon_days,
             :pred_direction, :prob_up, :prob_flat, :prob_down,
             :pred_confidence, :is_high_confidence,
             now(), :prediction_date, :target_date)
        ON CONFLICT (ticker_id, date_id, horizon_days, model_id) DO NOTHING
    """)
    with engine.begin() as conn:
        for i, (_, row) in enumerate(valid_df.iterrows()):
            date_id = get_date_id(conn, row["trade_date"])
            if not date_id:
                continue
            prob_up    = round(float(cls_proba[i][1]), 4)
            prob_down  = round(float(cls_proba[i][0]), 4)
            prob_flat  = round(max(1.0 - prob_up - prob_down, 0.0), 4)
            confidence = max(prob_up, prob_down)
            conn.execute(sql, {
                "ticker_id":          int(row["ticker_id"]),
                "date_id":            date_id,
                "model_id":           cls_model_id,
                "horizon_days":       horizon,
                "pred_direction":     1 if cls_preds[i] == 1 else -1,
                "prob_up":            prob_up,
                "prob_flat":          prob_flat,
                "prob_down":          prob_down,
                "pred_confidence":    round(confidence, 4),
                "is_high_confidence": confidence >= 0.70,
                "prediction_date":    prediction_date,
                "target_date":        prediction_date + timedelta(days=horizon),
            })
    log.info("Inserted %d classification predictions", len(valid_df))


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run(prediction_date=datetime.now().date(), horizon=1, tickers=None):
    engine = get_engine()

    # Resolve champion models
    cls_model_id, _ = get_champion(engine, "classification", horizon)
    reg_model_id, _ = get_champion(engine, "regression",     horizon)

    # Both share the same model_id from train.py (registered with same uuid)
    # Use cls_model_id as the shared key to locate artefacts
    cls_model, reg_model, scaler = load_artefacts(horizon, cls_model_id)

    # Load today's features
    df = load_features(engine, prediction_date, tickers)
    if df.empty:
        log.info("No features found for %s — run gold_transform.py first.", prediction_date)
        return

    # Inference
    valid_df, cls_preds, cls_proba, ret_preds = predict(df, cls_model, reg_model, scaler)
    if valid_df.empty:
        log.info("No valid rows to predict.")
        return

    # Write
    insert_classification_predictions(engine, valid_df, cls_preds, cls_proba,
                                       cls_model_id, horizon, prediction_date)
    insert_regression_predictions(engine, valid_df, ret_preds, cls_model_id,
                                   reg_model_id, horizon, prediction_date)

    log.info("=== Prediction complete  date=%s  horizon=%dd ===",
             prediction_date, horizon)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, default=date.today(), help="Prediction date YYYY-MM-DD")
    parser.add_argument("--horizon", type=int, default=1, choices=[1, 5, 20])
    parser.add_argument("--tickers", default=None, help="AAPL,MSFT,...")
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    run(args.date, args.horizon, tickers)