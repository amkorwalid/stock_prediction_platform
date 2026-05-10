"""
gold_transform.py
────────────────────────────────────────────────────────────────
Silver → Gold: fact_daily_prices → feat_technical + feat_price_dynamics

Reads the full price history from the rebuilt silver layer,
computes all features, then rebuilds the entire gold feature tables.

Usage:
    python gold_transform.py

Env var required:
    ETL_USER_USERNAME, ETL_USER_PASSWORD, DB_HOST, DB_PORT, DB_NAME
"""

import logging
import os
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


def get_engine():
    db_url = f"postgresql://{os.getenv('ETL_USER_USERNAME')}:{os.getenv('ETL_USER_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    return create_engine(db_url, pool_pre_ping=True)


# ── 1. Read Silver ────────────────────────────────────────────────────────────

def read_silver(engine):
    """
    Load the full price history for all tickers.
    We need complete history for rolling windows (up to 50 days).
    Returns a DataFrame sorted by ticker + trade_date.
    """
    sql = """
        SELECT
            f.ticker_id,
            f.date_id,
            d.trade_date,
            t.symbol,
            f.open,
            f.high,
            f.low,
            f.close,
            f.volume
        FROM data_warehouse.fact_daily_prices AS f
        JOIN data_warehouse.dim_date   AS d ON d.date_id   = f.date_id
        JOIN data_warehouse.dim_ticker AS t ON t.ticker_id = f.ticker_id
        ORDER BY t.symbol, d.trade_date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    log.info("Read %d rows from fact_daily_prices", len(df))
    return df


# ── 2. Compute feat_technical ─────────────────────────────────────────────────

def compute_technical(group: pd.DataFrame) -> pd.DataFrame:
    """
    All indicators computed on the full history, then rows for other dates
    are dropped after the merge in the caller.

    Shift rules (leak prevention):
      - Indicators that use today's close (RSI, MACD, OBV) are shifted by 1
        so the model only sees yesterday's signal when predicting today.
      - SMAs and EMAs are not shifted — they are price-level context,
        and the model is not predicting from them directly.
    """
    df = group.copy().reset_index(drop=True)
    c = df["close"]
    h = df["high"]
    lo = df["low"]
    v = df["volume"]

    # ── Moving averages ───────────────────────────────────────────────────────
    df["sma_5"]  = c.rolling(5).mean()
    df["sma_10"] = c.rolling(10).mean()
    df["sma_20"] = c.rolling(20).mean()
    df["sma_50"] = c.rolling(50).mean()
    df["ema_12"] = c.ewm(span=12, adjust=False).mean()
    df["ema_26"] = c.ewm(span=26, adjust=False).mean()

    # ── RSI (shifted to prevent leakage) ─────────────────────────────────────
    delta = c.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, np.nan)
    df["rsi_14"] = (100 - 100 / (1 + rs)).shift(1)

    # ── MACD (shifted) ────────────────────────────────────────────────────────
    raw_macd       = (df["ema_12"] - df["ema_26"]) / c.replace(0, np.nan)
    macd_signal    = raw_macd.ewm(span=9, adjust=False).mean()
    df["macd"]        = raw_macd.shift(1)
    df["macd_signal"] = macd_signal.shift(1)
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    bb_mean = c.rolling(20).mean()
    bb_std  = c.rolling(20).std()
    df["boll_mid"]   = bb_mean
    df["boll_upper"] = bb_mean + 2 * bb_std
    df["boll_lower"] = bb_mean - 2 * bb_std
    # Normalised 0-1 position within the band (shifted)
    band_width = 4 * bb_std
    df["bb_position"] = ((c - (bb_mean - 2 * bb_std)) / band_width.replace(0, np.nan)).shift(1)

    # ── ATR ───────────────────────────────────────────────────────────────────
    prev_close = c.shift(1)
    tr = pd.concat([
        h - lo,
        (h - prev_close).abs(),
        (lo - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()

    # ── OBV (shifted) ─────────────────────────────────────────────────────────
    direction = np.sign(c.diff()).fillna(0)
    df["obv"] = (v * direction).cumsum().shift(1).astype("Int64")

    # ── VWAP (running daily — typical price × volume / cumulative volume) ─────
    typical = (h + lo + c) / 3
    df["vwap"] = (typical * v).cumsum() / v.cumsum().replace(0, np.nan)

    return df


# ── 3. Compute feat_price_dynamics ────────────────────────────────────────────

def compute_price_dynamics(group: pd.DataFrame) -> pd.DataFrame:
    """
    All shift(n) features are pre-applied here — no further shifting at
    training time, as noted in the ERD.
    """
    df = group.copy().reset_index(drop=True)
    c  = df["close"]
    h  = df["high"]
    lo = df["low"]
    o  = df["open"]
    v  = df["volume"]

    # ── Returns ───────────────────────────────────────────────────────────────
    ret = c.pct_change()
    df["return_1d"]     = ret
    df["return_5d"]     = c.pct_change(5)
    df["return_20d"]    = c.pct_change(20)
    df["log_return_1d"] = np.log(c / c.shift(1))

    # ── Lagged returns ────────────────────────────────────────────────────────
    df["lag_1"]  = ret.shift(1)
    df["lag_2"]  = ret.shift(2)
    df["lag_3"]  = ret.shift(3)
    df["lag_7"]  = ret.shift(7)
    df["lag_20"] = ret.shift(20)

    # ── Rolling return averages ───────────────────────────────────────────────
    df["ma7"]  = ret.rolling(7).mean()
    df["ma30"] = ret.rolling(30).mean()

    # ── Momentum ──────────────────────────────────────────────────────────────
    df["momentum_3"] = ret - ret.shift(3)
    df["momentum_7"] = ret - ret.shift(7)

    # ── Rolling volatility ────────────────────────────────────────────────────
    df["vol_5d"]  = ret.rolling(5).std()
    df["vol_7d"]  = ret.rolling(7).std()
    df["vol_20d"] = ret.rolling(20).std()

    # ── Volatility of volatility (shifted) ────────────────────────────────────
    df["vol_of_vol"] = df["vol_7d"].rolling(7).std().shift(1)

    # ── Relative price signals ────────────────────────────────────────────────
    sma50 = c.rolling(50).mean()
    df["price_vs_sma50"] = (c - sma50) / sma50.replace(0, np.nan)
    df["hl_spread"]      = (h - lo) / c.replace(0, np.nan)
    df["daily_range"]    = ((h - lo) / lo.replace(0, np.nan)).shift(1)
    df["gap_open"]       = (o - c.shift(1)) / c.shift(1).replace(0, np.nan)

    # ── Volume signals ────────────────────────────────────────────────────────
    avg_vol_20 = v.rolling(20).mean()
    df["volume_ratio"]  = v / avg_vol_20.replace(0, np.nan)
    df["rel_volume"]    = (v - avg_vol_20) / avg_vol_20.replace(0, np.nan)
    # Avoid infinite values if prior volume was zero (replace inf with NaN)
    df["volume_change"] = v.pct_change().replace([np.inf, -np.inf], np.nan).shift(1)

    return df


# ── 4. Upsert helpers ─────────────────────────────────────────────────────────

def _val(row, col):
    """Return None for NaN/NaT or non-finite numeric, else the scalar value."""
    v = row.get(col)
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    try:
        # Treat infinite and non-finite numeric values as missing
        if np.isscalar(v) and not np.isfinite(v):
            return None
    except Exception:
        pass
    return v


def rewrite_technical(engine, df):
    """Replace feat_technical with the full computed dataset."""
    if df.empty:
        log.info("No computed technical features; skipping rewrite")
        return 0

    delete_sql = text("DELETE FROM data_warehouse.feat_technical")
    insert_sql = text("""
        INSERT INTO data_warehouse.feat_technical (
            ticker_id, date_id,
            sma_5, sma_10, sma_20, sma_50, ema_12, ema_26,
            rsi_14, macd, macd_signal, macd_hist,
            boll_upper, boll_lower, boll_mid, atr_14, bb_position,
            obv, vwap, computed_at
        ) VALUES (
            :ticker_id, :date_id,
            :sma_5, :sma_10, :sma_20, :sma_50, :ema_12, :ema_26,
            :rsi_14, :macd, :macd_signal, :macd_hist,
            :boll_upper, :boll_lower, :boll_mid, :atr_14, :bb_position,
            :obv, :vwap, now()
        )
    """)

    written = 0
    with engine.begin() as conn:
        conn.execute(delete_sql)
        log.info("Cleared existing feat_technical")

        for _, row in df.iterrows():
            conn.execute(insert_sql, {
                "ticker_id":   int(row["ticker_id"]),
                "date_id":     int(row["date_id"]),
                "sma_5":       _val(row, "sma_5"),
                "sma_10":      _val(row, "sma_10"),
                "sma_20":      _val(row, "sma_20"),
                "sma_50":      _val(row, "sma_50"),
                "ema_12":      _val(row, "ema_12"),
                "ema_26":      _val(row, "ema_26"),
                "rsi_14":      _val(row, "rsi_14"),
                "macd":        _val(row, "macd"),
                "macd_signal": _val(row, "macd_signal"),
                "macd_hist":   _val(row, "macd_hist"),
                "boll_upper":  _val(row, "boll_upper"),
                "boll_lower":  _val(row, "boll_lower"),
                "boll_mid":    _val(row, "boll_mid"),
                "atr_14":      _val(row, "atr_14"),
                "bb_position": _val(row, "bb_position"),
                "obv":         int(row["obv"]) if _val(row, "obv") is not None else None,
                "vwap":        _val(row, "vwap"),
            })
            written += 1
    log.info("Rebuilt feat_technical with %d rows", written)
    return written


def rewrite_price_dynamics(engine, df):
    """Replace feat_price_dynamics with the full computed dataset."""
    if df.empty:
        log.info("No computed price dynamics features; skipping rewrite")
        return 0

    delete_sql = text("DELETE FROM data_warehouse.feat_price_dynamics")
    insert_sql = text("""
        INSERT INTO data_warehouse.feat_price_dynamics (
            ticker_id, date_id,
            return_1d, lag_1, lag_2, lag_3, lag_7, lag_20,
            return_5d, return_20d, log_return_1d,
            ma7, ma30, momentum_3, momentum_7,
            vol_7d, vol_5d, vol_20d, vol_of_vol,
            price_vs_sma50, hl_spread, daily_range, gap_open,
            volume_ratio, rel_volume, volume_change,
            computed_at
        ) VALUES (
            :ticker_id, :date_id,
            :return_1d, :lag_1, :lag_2, :lag_3, :lag_7, :lag_20,
            :return_5d, :return_20d, :log_return_1d,
            :ma7, :ma30, :momentum_3, :momentum_7,
            :vol_7d, :vol_5d, :vol_20d, :vol_of_vol,
            :price_vs_sma50, :hl_spread, :daily_range, :gap_open,
            :volume_ratio, :rel_volume, :volume_change,
            now()
        )
    """)

    written = 0
    with engine.begin() as conn:
        conn.execute(delete_sql)
        log.info("Cleared existing feat_price_dynamics")

        for _, row in df.iterrows():
            conn.execute(insert_sql, {col: _val(row, col) for col in [
                "ticker_id", "date_id",
                "return_1d", "lag_1", "lag_2", "lag_3", "lag_7", "lag_20",
                "return_5d", "return_20d", "log_return_1d",
                "ma7", "ma30", "momentum_3", "momentum_7",
                "vol_7d", "vol_5d", "vol_20d", "vol_of_vol",
                "price_vs_sma50", "hl_spread", "daily_range", "gap_open",
                "volume_ratio", "rel_volume", "volume_change",
            ]} | {"ticker_id": int(row["ticker_id"]), "date_id": int(row["date_id"])})
            written += 1
    log.info("Rebuilt feat_price_dynamics with %d rows", written)
    return written


# ── 5. Orchestrator ───────────────────────────────────────────────────────────

def run():
    engine = get_engine()

    # Load full rebuilt silver layer
    df = read_silver(engine)
    if df.empty:
        log.info("No silver data found — exiting.")
        return

    tech_rows = []
    dyn_rows  = []

    for symbol, group in df.groupby("symbol"):
        # Compute features over the full history for this ticker
        tech = compute_technical(group)
        dyn  = compute_price_dynamics(group)

        # Collect all rows (no date filtering — rebuild entire tables)
        tech_rows.append(tech)
        dyn_rows.append(dyn)

    if not tech_rows:
        log.info("No features computed — exiting.")
        return

    all_tech = pd.concat(tech_rows, ignore_index=True)
    all_dyn  = pd.concat(dyn_rows, ignore_index=True)

    # Forward-fill sequential missing values per ticker (time-series imputation)
    def forward_fill_per_ticker(df, id_col="ticker_id"):
        if df.empty:
            return df
        df = df.copy()
        # numeric columns are the ones we want to propagate forward
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        # exclude identifier columns
        numeric_cols = [c for c in numeric_cols if c not in [id_col, "date_id"]]

        parts = []
        for _, g in df.groupby(id_col, sort=False):
            g = g.copy()
            g[numeric_cols] = g[numeric_cols].ffill()
            parts.append(g)
        return pd.concat(parts, ignore_index=True)

    all_tech = forward_fill_per_ticker(all_tech)
    all_dyn = forward_fill_per_ticker(all_dyn)

    rewrite_technical(engine, all_tech)
    rewrite_price_dynamics(engine, all_dyn)
    log.info("Gold transform complete — rebuilt all feature tables from full silver snapshot")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()