import os
from datetime import datetime

import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("ETL_USER_USERNAME"),
        password=os.getenv("ETL_USER_PASSWORD"),
    )

def pull_and_store(tickers: list[str], period: str = "1d", interval: str = "1d") -> int:

    if not tickers:
        print("No tickers provided — skipping.")
        return 0

    print("Downloading data for %d tickers: %s", len(tickers), tickers)

    # group_by="ticker" gives a clean (ticker, field) MultiIndex —
    # each ticker's OHLCV stays on its own row after stack()
    raw = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker"
    )

    if raw.empty:
        print("yfinance returned no data.")
        return 0

    # ── Reshape: (date, ticker) → one row per ticker per date ────────────────
    if isinstance(raw.columns, pd.MultiIndex):
        # Normal multi-ticker path
        df = raw.stack(level=0)           # index: (Date, Ticker)
        df.index.names = ["trade_date", "ticker"]
    else:
        # Single-ticker fallback: columns are flat, no ticker level
        ticker = tickers[0]
        df = raw.copy()
        df.index.name = "trade_date"
        df["ticker"] = ticker
        df = df.set_index("ticker", append=True)
        df.index.names = ["trade_date", "ticker"]

    df = df.reset_index()
    # Normalise column names (yfinance capitalises them)
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    # Keep only the columns we need; drop rows with any null OHLCV value
    required = ["ticker", "trade_date", "open", "high", "low", "close", "volume"]
    df = df[required].dropna(subset=required)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["ingested_at"] = datetime.now()
    
    if df.empty:
        print("No valid rows after cleaning.")
        return 0

    print("Inserting %d rows into raw_ohlcv_prices …", len(df))

    # ── Bulk insert ───────────────────────────────────────────────────────────
    rows = [
        (
            row.ticker,
            row.trade_date,
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            int(row.volume),
            row.ingested_at,
        )
        for row in df.itertuples(index=False)
    ]

    insert_sql = """
        INSERT INTO data_warehouse.raw_ohlcv_prices
            (ticker, trade_date, open, high, low, close, volume, ingested_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=500)
        print("Done — %d rows inserted.", len(rows))
        return len(rows)
    finally:
        conn.close()



pull_and_store(["AAPL", "MSFT", "TSLA", "GOOGL"], period="max")
