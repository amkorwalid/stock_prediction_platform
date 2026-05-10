"""
fetch_news_operator.py
~~~~~~~~~~~~~~~~~~~~~~
Pulls financial news articles from Alpha Vantage for each configured ticker
and writes raw records into data_warehouse.raw_news_articles (Bronze layer).

Airflow Variables expected
--------------------------
TICKERS               Comma-separated ticker list, e.g. "AAPL,MSFT,TSLA"
ALPHA_VANTAGE_KEY     API key for Alpha Vantage

Airflow Connections expected
----------------------------
postgres_stock   PostgreSQL connection to the platform database.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

import psycopg2.extras
import requests
from airflow.models import Variable
from airflow.utils.context import Context

from plugins.base_operator import StockBaseOperator

# Alpha Vantage free tier: 5 requests/minute, 500/day.
_AV_BASE = "https://www.alphavantage.co/query"
_REQUEST_DELAY_SEC = 13   # ~5 req/min with safety margin


class FetchNewsOperator(StockBaseOperator):
    """
    Fetches financial news articles from Alpha Vantage and writes them
    into data_warehouse.raw_news_articles (Bronze layer).

    Parameters
    ----------
    tickers :
        Optional explicit list of symbols. Falls back to Airflow Variable
        ``TICKERS``.
    lookback_days :
        Articles published within this many days before the execution date
        are fetched. Default 1 (today's news window for a daily pipeline).
    max_articles_per_ticker :
        Hard cap per ticker per run to stay within API limits. Default 50.
    """

    ui_color = "#F5C4B3"   # coral-100 — Bronze layer

    required_variables = ["TICKERS", "ALPHA_VANTAGE_KEY"]

    def __init__(
        self,
        *,
        tickers: list[str] | None = None,
        lookback_days: int = 1,
        max_articles_per_ticker: int = 50,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="fetch_news", **kwargs)
        self._tickers = tickers
        self.lookback_days = lookback_days
        self.max_articles_per_ticker = max_articles_per_ticker

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        tickers = self._resolve_tickers()
        api_key = Variable.get("ALPHA_VANTAGE_KEY")
        exec_date = self._execution_date(context)
        time_from = exec_date - timedelta(days=self.lookback_days)

        log.info(
            "Fetching news for %d ticker(s), window %s → %s",
            len(tickers),
            time_from,
            exec_date,
        )

        total_written = 0
        for i, ticker in enumerate(tickers):
            articles = self._fetch_articles(ticker, time_from, exec_date, api_key, log)
            written = self._upsert_articles(conn, articles, log)
            total_written += written
            log.info("  %-6s → %d article(s) written", ticker, written)

            # Rate-limit: sleep between tickers except after the last one
            if i < len(tickers) - 1:
                time.sleep(_REQUEST_DELAY_SEC)

        log.info("News fetch complete. Total articles written: %d", total_written)
        return total_written

    # ── private helpers ───────────────────────────────────────────────────────

    def _resolve_tickers(self) -> list[str]:
        if self._tickers:
            return [t.strip().upper() for t in self._tickers]
        raw = Variable.get("TICKERS", default_var="")
        tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
        if not tickers:
            raise ValueError("No tickers configured. Set the 'TICKERS' Airflow Variable.")
        return tickers

    @staticmethod
    def _execution_date(context: Context) -> date:
        ds = context.get("ds")
        if ds:
            return datetime.strptime(ds, "%Y-%m-%d").date()
        return datetime.now(timezone.utc).date()

    def _fetch_articles(
        self,
        ticker: str,
        time_from: date,
        time_to: date,
        api_key: str,
        log: logging.Logger,
    ) -> list[dict]:
        """Call Alpha Vantage NEWS_SENTIMENT endpoint and parse the response."""
        # AV expects format YYYYMMDDTHHMM
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ticker,
            "time_from": time_from.strftime("%Y%m%dT0000"),
            "time_to": time_to.strftime("%Y%m%dT2359"),
            "limit": self.max_articles_per_ticker,
            "apikey": api_key,
        }

        try:
            resp = requests.get(_AV_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("Alpha Vantage request failed for %s: %s", ticker, exc)
            return []

        if "feed" not in data:
            log.warning(
                "No 'feed' key in Alpha Vantage response for %s. "
                "Possible rate-limit or invalid key. Response: %s",
                ticker,
                str(data)[:200],
            )
            return []

        articles = []
        for item in data["feed"]:
            # Collect all tickers mentioned in this article
            mentioned = [
                ts["ticker"]
                for ts in item.get("ticker_sentiment", [])
                if ts.get("ticker")
            ]
            if not mentioned:
                mentioned = [ticker]

            try:
                published_at = datetime.strptime(
                    item["time_published"], "%Y%m%dT%H%M%S"
                ).replace(tzinfo=timezone.utc)
            except (KeyError, ValueError):
                published_at = datetime.now(timezone.utc)

            articles.append(
                {
                    "article_id": str(uuid4()),
                    "tickers": mentioned,
                    "headline": item.get("title", "")[:2000],
                    "body": item.get("summary", ""),
                    "source": item.get("source", "unknown")[:255],
                    "published_at": published_at,
                    "lang": "en",
                    "ingested_at": datetime.now(timezone.utc),
                }
            )
        return articles

    @staticmethod
    def _upsert_articles(
        conn: psycopg2.extensions.connection,
        articles: list[dict],
        log: logging.Logger,
    ) -> int:
        """
        Insert articles into raw_news_articles.
        ON CONFLICT on article_id → DO NOTHING (idempotent re-runs).
        tickers is stored as a native Postgres text array.
        """
        if not articles:
            return 0

        sql = """
            INSERT INTO data_warehouse.raw_news_articles
                (article_id, tickers, headline, body, source,
                 published_at, lang, ingested_at)
            VALUES
                (%(article_id)s, %(tickers)s, %(headline)s, %(body)s,
                 %(source)s, %(published_at)s, %(lang)s, %(ingested_at)s)
            ON CONFLICT (article_id) DO NOTHING
        """

        # psycopg2 needs an explicit list→array cast for varchar[]
        records = []
        for a in articles:
            rec = dict(a)
            rec["tickers"] = psycopg2.extras.Json(None)  # placeholder — overwritten below
            records.append(rec)

        try:
            with conn.cursor() as cur:
                for article in articles:
                    cur.execute(
                        sql,
                        {
                            **article,
                            # Cast Python list → Postgres text array
                            "tickers": psycopg2.extensions.AsIs(
                                "ARRAY[%s]::varchar[]"
                                % ",".join(f"'{t}'" for t in article["tickers"])
                            ),
                        },
                    )
            return len(articles)
        except Exception as exc:
            log.error("Failed to upsert news articles: %s", exc)
            raise
