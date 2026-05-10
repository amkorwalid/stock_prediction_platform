"""
fetch_ohlcv_operator.py
~~~~~~~~~~~~~~~~~~~~~~~
Pulls end-of-day OHLCV bars from Yahoo Finance for one or more tickers
and writes raw records into data_warehouse.raw_ohlcv_prices (Bronze layer).

Airflow Variables expected
--------------------------
TICKERS       Comma-separated list, e.g. "AAPL,MSFT,TSLA"

Airflow Connections expected
----------------------------
postgres_stock   PostgreSQL connection to the platform database.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2.extras
import yfinance as yf
from airflow.models import Variable
from airflow.utils.context import Context

from plugins.base_operator import StockBaseOperator


class FetchOHLCVOperator(StockBaseOperator):
    """
    Downloads OHLCV bars from Yahoo Finance and writes them to
    data_warehouse.raw_ohlcv_prices.

    Parameters
    ----------
    tickers :
        Optional explicit list of ticker symbols. Falls back to the
        Airflow Variable ``TICKERS`` when not provided.
    lookback_days :
        How many calendar days of history to fetch on each run.
        Default is 1 (yesterday's close, appropriate for a daily schedule).
        Use a larger value for backfill runs.
    """

    ui_color = "#F5C4B3"  # coral-100 — matches Bronze in the architecture diagram

    required_variables = ["TICKERS"]

    def __init__(
        self,
        *,
        tickers: list[str] | None = None,
        lookback_days: int = 1,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="fetch_ohlcv", **kwargs)
        self._tickers = tickers
        self.lookback_days = lookback_days

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        tickers = self._resolve_tickers()
        end_date = self._execution_date(context)
        start_date = end_date - timedelta(days=self.lookback_days)

        log.info(
            "Fetching OHLCV for %d ticker(s) from %s to %s",
            len(tickers),
            start_date,
            end_date,
        )

        total_written = 0
        for ticker in tickers:
            rows = self._fetch_ticker(ticker, start_date, end_date, log)
            written = self._upsert_rows(conn, rows, log)
            total_written += written
            log.info("  %-6s → %d row(s) written", ticker, written)

        log.info("OHLCV fetch complete. Total rows written: %d", total_written)
        return total_written

    # ── private helpers ───────────────────────────────────────────────────────

    def _resolve_tickers(self) -> list[str]:
        if self._tickers:
            return [t.strip().upper() for t in self._tickers]
        raw = Variable.get("TICKERS", default_var="")
        tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
        if not tickers:
            raise ValueError(
                "No tickers configured. Set the 'TICKERS' Airflow Variable "
                "or pass tickers= to FetchOHLCVOperator."
            )
        return tickers

    @staticmethod
    def _execution_date(context: Context) -> date:
        """Return the logical execution date as a plain date."""
        ds = context.get("ds")  # "YYYY-MM-DD" string provided by Airflow
        if ds:
            return datetime.strptime(ds, "%Y-%m-%d").date()
        return datetime.now(timezone.utc).date()

    @staticmethod
    def _fetch_ticker(
        ticker: str,
        start: date,
        end: date,
        log: logging.Logger,
    ) -> list[dict]:
        """Download bars from Yahoo Finance and return them as plain dicts."""
        try:
            df = yf.download(
                ticker,
                start=start.isoformat(),
                # yfinance end is exclusive — add one day
                end=(end + timedelta(days=1)).isoformat(),
                progress=False,
                auto_adjust=False,
            )
        except Exception as exc:
            log.warning("yfinance download failed for %s: %s", ticker, exc)
            return []

        if df.empty:
            log.warning("No data returned by Yahoo Finance for %s", ticker)
            return []

        rows = []
        for trade_date, row in df.iterrows():
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": trade_date.date(),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "volume": int(row["Volume"]),
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        return rows

    @staticmethod
    def _upsert_rows(
        conn: psycopg2.extensions.connection,
        rows: list[dict],
        log: logging.Logger,
    ) -> int:
        """
        Bulk-insert rows into raw_ohlcv_prices.

        Strategy: INSERT … ON CONFLICT (ticker, trade_date) DO UPDATE so that
        re-running a DAG for the same date is always safe and idempotent.
        """
        if not rows:
            return 0

        sql = """
            INSERT INTO data_warehouse.raw_ohlcv_prices
                (ticker, trade_date, open, high, low, close, volume, ingested_at)
            VALUES
                (%(ticker)s, %(trade_date)s, %(open)s, %(high)s,
                 %(low)s, %(close)s, %(volume)s, %(ingested_at)s)
            ON CONFLICT (ticker, trade_date)
            DO UPDATE SET
                open        = EXCLUDED.open,
                high        = EXCLUDED.high,
                low         = EXCLUDED.low,
                close       = EXCLUDED.close,
                volume      = EXCLUDED.volume,
                ingested_at = EXCLUDED.ingested_at
        """
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            return len(rows)
        except Exception as exc:
            log.error("Failed to upsert OHLCV rows: %s", exc)
            raise
