"""
scripts/fetching/news_fetcher.py
---------------------------------
Fetches daily financial news from Alpha Vantage for a list of tickers
and upserts results into data_warehouse.raw_news_articles.

Usage (standalone):
    python news_fetcher.py --tickers AAPL,MSFT,TSLA --date 2025-05-09

Usage (Airflow PythonOperator):
    from scripts.fetching.news_fetcher import run
    run(tickers=["AAPL", "MSFT"], target_date=date(2025, 5, 9),
        api_key=Variable.get("ALPHAVANTAGE_API_KEY"))

.env file (place next to script or at project root):
    ALPHAVANTAGE_API_KEY=your_key
    DB_HOST=your-db-private-ip
    DB_PORT=5432
    DB_NAME=stockdb
    ETL_USER_USERNAME=etl_user
    ETL_USER_PASSWORD=secret
    TICKERS=AAPL,MSFT,TSLA
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news_fetcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AV_BASE_URL   = "https://www.alphavantage.co/query"
AV_FUNCTION   = "NEWS_SENTIMENT"
AV_LIMIT      = 100   # articles per ticker per call (AV max = 1000)
REQUEST_DELAY = 12    # seconds between requests (safe for free tier; lower on premium)

# ---------------------------------------------------------------------------
# Database connection  (reads from .env / environment variables)
# ---------------------------------------------------------------------------


def get_connection() -> psycopg2.extensions.connection:
    """
    Open and return a psycopg2 connection using individual env vars.
    Registers the UUID adapter and sets autocommit=False.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("ETL_USER_USERNAME"),
        password=os.getenv("ETL_USER_PASSWORD"),
    )
    psycopg2.extras.register_uuid(conn_or_curs=conn)
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Alpha Vantage API
# ---------------------------------------------------------------------------


def _parse_av_timestamp(av_time: str) -> datetime | None:
    """Convert Alpha Vantage '20240501T130000' to a UTC-aware datetime."""
    try:
        return datetime.strptime(av_time, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def fetch_news_for_ticker(
    ticker: str,
    api_key: str,
    target_date: date,
    limit: int = AV_LIMIT,
) -> list[dict[str, Any]]:
    """
    Call Alpha Vantage NEWS_SENTIMENT for *ticker* on *target_date*.

    Returns the raw 'feed' list from the API response.
    Raises RuntimeError on API-level errors (rate limit, bad key).
    Raises requests.HTTPError on HTTP-level failures.
    """
    time_from = datetime(target_date.year, target_date.month, target_date.day, 0, 0)
    time_to   = time_from + timedelta(days=1)

    params = {
        "function":  AV_FUNCTION,
        "tickers":   ticker,
        "time_from": time_from.strftime("%Y%m%dT%H%M"),
        "time_to":   time_to.strftime("%Y%m%dT%H%M"),
        "limit":     limit,
        "sort":      "LATEST",
        "apikey":    api_key,
    }

    log.info("Fetching news | ticker=%s date=%s", ticker, target_date)
    resp = requests.get(AV_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()

    # Alpha Vantage signals problems via these top-level keys
    if "Information" in payload:
        raise RuntimeError(f"Alpha Vantage API info: {payload['Information']}")
    if "Note" in payload:
        raise RuntimeError(f"Alpha Vantage rate-limit note: {payload['Note']}")
    if "Error Message" in payload:
        raise RuntimeError(f"Alpha Vantage error: {payload['Error Message']}")

    articles = payload.get("feed", [])
    log.info("Received %d articles for %s", len(articles), ticker)
    return articles


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _extract_mentioned_tickers(feed_item: dict) -> list[str]:
    """Pull ticker symbols from the ticker_sentiment array."""
    return [
        ts["ticker"]
        for ts in feed_item.get("ticker_sentiment", [])
        if "ticker" in ts
    ]


def normalise_article(
    feed_item: dict,
    requested_ticker: str,
) -> dict[str, Any] | None:
    """
    Map one Alpha Vantage feed item to a dict matching raw_news_articles columns.

    Returns None when required fields (headline, source, published_at) are missing
    so callers can safely filter with [a for a in ... if a is not None].
    """
    headline = (feed_item.get("title") or "").strip()
    source   = (feed_item.get("source") or "").strip()
    av_time  = feed_item.get("time_published", "")

    if not headline or not source or not av_time:
        log.debug("Skipping article – missing required field: %s", feed_item.get("url"))
        return None

    published_at = _parse_av_timestamp(av_time)
    if published_at is None:
        log.debug("Skipping article – bad timestamp '%s': %s", av_time, feed_item.get("url"))
        return None

    # Always include the requested ticker; add any others mentioned in the article
    tickers = _extract_mentioned_tickers(feed_item)
    if requested_ticker not in tickers:
        tickers.insert(0, requested_ticker)

    return {
        "article_id":   str(uuid.uuid4()),
        "tickers":      tickers,
        "headline":     headline,
        "body":         (feed_item.get("summary") or "").strip() or None,
        "source":       source,
        "published_at": published_at,
        "lang":         (feed_item.get("language") or "en")[:8],
        "ingested_at":  datetime.now(timezone.utc),
        # Internal dedup key – not written to the DB
        "_url":         feed_item.get("url", ""),
    }


# ---------------------------------------------------------------------------
# In-memory deduplication
# ---------------------------------------------------------------------------


def deduplicate_by_url(articles: list[dict]) -> list[dict]:
    """
    Remove articles with duplicate URLs within the current batch.

    The same news story often appears in feeds for multiple tickers
    (e.g. an Apple-Microsoft joint announcement). Deduplicating here means
    we insert it once (under one article_id) instead of twice.

    Database-level safety is still provided by ON CONFLICT DO NOTHING.
    """
    seen: set[str] = set()
    result: list[dict] = []
    for article in articles:
        url = article.get("_url", "")
        if url and url in seen:
            log.debug("Dedup: skipping duplicate URL %s", url)
            continue
        if url:
            seen.add(url)
        result.append(article)
    return result


# ---------------------------------------------------------------------------
# Database insert
# ---------------------------------------------------------------------------

_INSERT_SQL = """
    INSERT INTO data_warehouse.raw_news_articles
        (article_id, tickers, headline, body, source, published_at, lang, ingested_at)
    VALUES
        (%(article_id)s, %(tickers)s, %(headline)s, %(body)s,
         %(source)s, %(published_at)s, %(lang)s, %(ingested_at)s)
    ON CONFLICT DO NOTHING
"""


def insert_articles(
    conn: psycopg2.extensions.connection,
    articles: list[dict[str, Any]],
) -> int:
    """
    Bulk-insert normalised articles into raw_news_articles.

    Uses ON CONFLICT DO NOTHING so the script is safe to re-run for the same
    date (idempotent).  psycopg2 automatically adapts Python lists to
    Postgres varchar[] arrays.

    Returns the number of rows passed to the DB.
    """
    if not articles:
        log.info("No articles to insert")
        return 0

    # Strip internal keys (prefixed with '_') before building DB rows
    db_rows = [
        {k: v for k, v in a.items() if not k.startswith("_")}
        for a in articles
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _INSERT_SQL, db_rows, page_size=100)

    conn.commit()
    log.info("Committed %d articles to raw_news_articles", len(db_rows))
    return len(db_rows)


# ---------------------------------------------------------------------------
# Main pipeline function (callable from Airflow or CLI)
# ---------------------------------------------------------------------------


def run(
    tickers: list[str],
    target_date: date,
    api_key: str,
) -> dict[str, int]:
    """
    Fetch news for every ticker and persist to the database.

    Connection is opened via get_connection() which reads DB credentials
    from environment variables / .env file.

    Returns a per-ticker article count dict, e.g.:
        {"AAPL": 12, "MSFT": 8, "TSLA": 5}

    Raises on fatal errors (bad credentials, DB connection failure)
    so the Airflow task is correctly marked as failed.
    """
    conn = get_connection()
    log.info("Database connection established")

    all_articles: list[dict] = []
    per_ticker_counts: dict[str, int] = {}

    for idx, ticker in enumerate(tickers):
        # Respect Alpha Vantage rate limits between requests
        if idx > 0:
            log.info("Waiting %ss before next API call (rate limit)", REQUEST_DELAY)
            time.sleep(REQUEST_DELAY)

        try:
            raw_feed = fetch_news_for_ticker(ticker, api_key, target_date)
        except Exception as exc:
            log.error("Could not fetch news for %s: %s", ticker, exc)
            per_ticker_counts[ticker] = 0
            continue

        normalised = [normalise_article(item, ticker) for item in raw_feed]
        valid = [a for a in normalised if a is not None]

        skipped = len(raw_feed) - len(valid)
        if skipped:
            log.warning("%s: skipped %d articles with missing/invalid fields", ticker, skipped)

        per_ticker_counts[ticker] = len(valid)
        all_articles.extend(valid)

    # Deduplicate cross-ticker duplicates before hitting the DB
    n_before = len(all_articles)
    all_articles = deduplicate_by_url(all_articles)
    log.info(
        "Cross-ticker dedup: %d → %d unique articles",
        n_before, len(all_articles),
    )

    insert_articles(conn, all_articles)
    conn.close()

    return per_ticker_counts


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch daily news from Alpha Vantage and store in raw_news_articles",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--tickers",
        default=os.getenv("TICKERS", ""),
        help="Comma-separated list of ticker symbols (e.g. AAPL,MSFT,TSLA). "
             "Falls back to the TICKERS env var.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target date in YYYY-MM-DD format. Defaults to yesterday.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()

    # -- Resolve API key
    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not api_key:
        log.error("ALPHAVANTAGE_API_KEY is not set in environment or .env file")
        sys.exit(1)

    # -- Resolve tickers
    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    if not tickers:
        log.error("No tickers supplied. Use --tickers AAPL,MSFT or set TICKERS env var.")
        sys.exit(1)

    # -- Resolve target date
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid date format '%s' – expected YYYY-MM-DD", args.date)
            sys.exit(1)
    else:
        target_date = date.today() - timedelta(days=1)

    log.info(
        "Starting | tickers=%s | date=%s | limit=%d articles/ticker",
        ",".join(tickers), target_date, AV_LIMIT,
    )

    summary = run(tickers, target_date, api_key)

    total = sum(summary.values())
    log.info("Finished | total unique articles inserted: %d", total)
    for ticker, count in summary.items():
        log.info("  %-8s %3d articles", ticker, count)


if __name__ == "__main__":
    main()