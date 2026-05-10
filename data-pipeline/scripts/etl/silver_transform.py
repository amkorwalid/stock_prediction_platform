"""
Usage:
    python silver_transform.py
"""

import logging
import os
from datetime import date, timedelta
import calendar

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


def get_engine():
    db_url = f"postgresql://{os.getenv('ETL_USER_USERNAME')}:{os.getenv('ETL_USER_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT', 5432)}/{os.getenv('DB_NAME')}"
    return create_engine(db_url, pool_pre_ping=True)


# ── 1. Read Bronze ────────────────────────────────────────────────────────────

def read_raw(engine):
    sql = """
        SELECT id, ticker, trade_date, open, high, low, close, volume, ingested_at
        FROM data_warehouse.raw_ohlcv_prices
        ORDER BY ticker, trade_date, ingested_at, id
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
    log.info("Read %d raw rows", len(df))
    return df


def dedupe_raw(df):
    """Keep the latest bronze row for each ticker/date pair."""
    if df.empty:
        return df

    cleaned = (
        df.sort_values(["ticker", "trade_date", "ingested_at", "id"])
          .drop_duplicates(subset=["ticker", "trade_date"], keep="last")
          .copy()
    )
    cleaned["trade_date"] = pd.to_datetime(cleaned["trade_date"]).dt.date
    cleaned = cleaned.drop(columns=["id", "ingested_at"], errors="ignore")
    log.info("Deduplicated bronze rows from %d to %d", len(df), len(cleaned))
    return cleaned


# ── 2. Upsert dim_ticker ──────────────────────────────────────────────────────

def upsert_tickers(engine, symbols):
    """Insert new symbols using yfinance metadata. Returns {symbol: ticker_id}."""
    with engine.connect() as conn:
        existing = pd.read_sql(
            text("SELECT symbol, ticker_id FROM data_warehouse.dim_ticker WHERE symbol = ANY(:s)"),
            conn, params={"s": symbols}
        )
    ticker_map = dict(zip(existing["symbol"], existing["ticker_id"]))

    new_symbols = [s for s in symbols if s not in ticker_map]
    if new_symbols:
        log.info("New tickers to insert: %s", new_symbols)

    insert_sql = text("""
        INSERT INTO data_warehouse.dim_ticker (symbol, company, sector, industry, exchange, currency, is_active, updated_at)
        VALUES (:symbol, :company, :sector, :industry, :exchange, :currency, true, now())
        ON CONFLICT (symbol) DO NOTHING
        RETURNING symbol, ticker_id
    """)

    with engine.begin() as conn:
        for symbol in new_symbols:
            try:
                info = yf.Ticker(symbol).info
            except Exception:
                info = {}
            row = conn.execute(insert_sql, {
                "symbol":   symbol,
                "company":  info.get("longName") or info.get("shortName") or symbol,
                "sector":   info.get("sector"),
                "industry": info.get("industry"),
                "exchange": info.get("exchange") or "UNKNOWN",
                "currency": info.get("currency") or "USD",
            }).fetchone()
            if row:
                ticker_map[row[0]] = row[1]

    # Catch any DO NOTHING misses (e.g. concurrent insert)
    missing = [s for s in symbols if s not in ticker_map]
    if missing:
        with engine.connect() as conn:
            refetch = pd.read_sql(
                text("SELECT symbol, ticker_id FROM data_warehouse.dim_ticker WHERE symbol = ANY(:s)"),
                conn, params={"s": missing}
            )
        ticker_map.update(dict(zip(refetch["symbol"], refetch["ticker_id"])))

    return ticker_map


# ── 3. Upsert dim_date ────────────────────────────────────────────────────────

def upsert_dates(engine, dates):
    """Insert any date rows missing from dim_date. Returns {date: date_id}."""
    with engine.connect() as conn:
        existing = pd.read_sql(
            text("SELECT trade_date, date_id FROM data_warehouse.dim_date WHERE trade_date = ANY(:d)"),
            conn, params={"d": dates}
        )
    existing["trade_date"] = pd.to_datetime(existing["trade_date"]).dt.date
    date_map = dict(zip(existing["trade_date"], existing["date_id"]))

    insert_sql = text("""
        INSERT INTO data_warehouse.dim_date
            (trade_date, year, quarter, month, week_of_year, day_of_week, day_of_month,
            is_trading_day, is_month_end, is_quarter_end)
        VALUES
            (:trade_date, :year, :quarter, :month, :week_of_year, :day_of_week, :day_of_month,
            :is_trading_day, :is_month_end, :is_quarter_end)
        ON CONFLICT (trade_date) DO NOTHING
        RETURNING trade_date, date_id
    """)

    
    with engine.begin() as conn:
        for d in [d for d in dates if d not in date_map]:
            last_day = calendar.monthrange(d.year, d.month)[1]
            is_month_end = d.day == last_day
            row = conn.execute(insert_sql, {
                "trade_date":    d,
                "year":          d.year,
                "quarter":       (d.month - 1) // 3 + 1,
                "month":         d.month,
                "week_of_year":  d.isocalendar()[1],
                "day_of_week":   d.strftime("%A"),
                "day_of_month":  d.day,
                "is_trading_day": d.weekday() < 5,
                "is_month_end":  is_month_end,
                "is_quarter_end": is_month_end and d.month in (3, 6, 9, 12),
            }).fetchone()
            if row:
                date_map[row[0]] = row[1]

    return date_map


# ── 4. Fetch adj_close + vwap from yfinance ───────────────────────────────────

def compute_adjusted(df):
    """
    Adds adj_close and vwap columns directly from raw OHLCV data.

    adj_close : same as close 
    vwap      : typical price = (high + low + close) / 3
    """
    df["adj_close"] = df["close"]
    df["vwap"] = ((df["high"] + df["low"] + df["close"]) / 3).round(6)
    return df


def rewrite_fact_table(engine, df, ticker_map, date_map):
    """Replace the silver price fact with the cleaned full bronze snapshot."""
    if df.empty:
        log.info("No cleaned rows available; skipping fact rewrite")
        return 0

    delete_sql = text("DELETE FROM data_warehouse.fact_daily_prices")
    insert_sql = text("""
        INSERT INTO data_warehouse.fact_daily_prices
            (ticker_id, date_id, open, high, low, close, adj_close, volume, vwap)
        VALUES
            (:ticker_id, :date_id, :open, :high, :low, :close, :adj_close, :volume, :vwap)
    """)

    written = 0
    with engine.begin() as conn:
        conn.execute(delete_sql)
        log.info("Cleared existing fact_daily_prices")

        for _, row in df.iterrows():
            sym = row["ticker"]
            td = row["trade_date"] if isinstance(row["trade_date"], date) else row["trade_date"].date()
            ticker_id = ticker_map.get(sym)
            date_id = date_map.get(td)

            if not ticker_id or not date_id:
                log.warning("Skipping %s %s — missing dimension key", sym, td)
                continue

            conn.execute(insert_sql, {
                "ticker_id": ticker_id,
                "date_id": date_id,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "adj_close": float(row["adj_close"]),
                "volume": int(row["volume"]),
                "vwap": float(row["vwap"]),
            })
            written += 1

    log.info("Rebuilt fact_daily_prices with %d rows", written)
    return written


# ── 5. Upsert fact_daily_prices ───────────────────────────────────────────────
def upsert_facts(engine, df, ticker_map, date_map):
    upsert_sql = text("""
        INSERT INTO data_warehouse.fact_daily_prices
            (ticker_id, date_id, open, high, low, close, adj_close, volume, vwap)
        VALUES
            (:ticker_id, :date_id, :open, :high, :low, :close, :adj_close, :volume, :vwap)
        ON CONFLICT (ticker_id, date_id) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, adj_close=EXCLUDED.adj_close,
            volume=EXCLUDED.volume, vwap=EXCLUDED.vwap
    """)

    written = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            sym = row["ticker"]
            td  = row["trade_date"] if isinstance(row["trade_date"], date) else row["trade_date"].date()
            ticker_id = ticker_map.get(sym)
            date_id   = date_map.get(td)

            if not ticker_id or not date_id:
                log.warning("Skipping %s %s — missing dimension key", sym, td)
                continue

            conn.execute(upsert_sql, {
                "ticker_id": ticker_id,
                "date_id":   date_id,
                "open":      float(row["open"]),
                "high":      float(row["high"]),
                "low":       float(row["low"]),
                "close":     float(row["close"]),
                "adj_close": float(row["adj_close"]),
                "volume":    int(row["volume"]),
                "vwap":      float(row["vwap"]),
            })
            written += 1

    log.info("Upserted %d rows into fact_daily_prices", written)
    return written

# ── Orchestrator ──────────────────────────────────────────────────────────────

def run():
    engine = get_engine()

    df = dedupe_raw(read_raw(engine))
    if df.empty:
        log.info("Nothing to transform from bronze")
        return

    df = compute_adjusted(df)

    symbols    = df["ticker"].unique().tolist()
    dates      = df["trade_date"].apply(lambda d: d if isinstance(d, date) else d.date()).unique().tolist()
    ticker_map = upsert_tickers(engine, symbols)
    date_map   = upsert_dates(engine, dates)

    rewrite_fact_table(engine, df, ticker_map, date_map)
    log.info("Silver transform complete for full bronze snapshot")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run()