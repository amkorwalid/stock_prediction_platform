"""
------------------------------------------------
Bronze → Silver transform for news articles.

Reads from  : data_warehouse.raw_news_articles  (Bronze)
Writes to   : data_warehouse.fact_news_articles  (Silver)

What this script does
---------------------
1. Fetch raw articles for *target_date* that haven't been promoted yet.
2. Clean & validate each article:
     - Strip HTML tags and normalise whitespace in body text
     - Upper-case and validate ticker symbols against dim_ticker
     - Ensure published_at is UTC-aware and falls on target_date
     - Drop articles missing headline or source
3. Explode: one raw row (tickers=[AAPL, MSFT]) → two Silver rows (one per ticker)
4. Resolve foreign keys:
     - ticker symbol  → ticker_id  (via dim_ticker)
     - published_at   → date_id    (via dim_date on the article's publish date)
5. Deduplicate:
     - Within batch  : (article_id, ticker_id) pairs — same article, same ticker
     - Cross-batch   : ON CONFLICT DO NOTHING on fact_news_articles.article_id (PK)
6. Compute word_count from cleaned body text.
7. Bulk-insert into fact_news_articles.

Usage (standalone):
    python news_cleaner.py --date 2025-05-09

Usage (Airflow PythonOperator):
    from scripts.preprocessing.silver_news_transform import run
    run(target_date=date(2025, 5, 9))

.env:
    DB_HOST, DB_PORT, DB_NAME, ETL_USER_USERNAME, ETL_USER_PASSWORD
"""

from __future__ import annotations

import argparse
import html
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras
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
log = logging.getLogger("silver_news_transform")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def get_connection() -> psycopg2.extensions.connection:
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
# SQL queries
# ---------------------------------------------------------------------------

# Fetch raw articles published on target_date that are not yet in Silver.
# We filter on the DATE portion of published_at (cast to the DB's local date).
_FETCH_RAW_SQL = """
    SELECT
        r.article_id,
        r.tickers,
        r.headline,
        r.body,
        r.source,
        r.published_at,
        r.lang
    FROM data_warehouse.raw_news_articles r
    WHERE r.published_at::date = %(target_date)s
      AND NOT EXISTS (
          SELECT 1
          FROM data_warehouse.fact_news_articles f
          WHERE f.article_id = r.article_id
      )
    ORDER BY r.published_at
"""

# Resolve ticker symbols to ticker_ids in one round-trip.
_RESOLVE_TICKERS_SQL = """
    SELECT symbol, ticker_id
    FROM   data_warehouse.dim_ticker
    WHERE  symbol = ANY(%(symbols)s)
      AND  is_active = TRUE
"""

# Resolve a set of dates to date_ids in one round-trip.
_RESOLVE_DATES_SQL = """
    SELECT trade_date, date_id
    FROM   data_warehouse.dim_date
    WHERE  trade_date = ANY(%(dates)s)
"""

_INSERT_SILVER_SQL = """
    INSERT INTO data_warehouse.fact_news_articles
        (article_id, ticker_id, date_id,
         headline, body_clean, published_at, lang, word_count, created_at)
    VALUES
        (%(article_id)s, %(ticker_id)s, %(date_id)s,
         %(headline)s, %(body_clean)s, %(published_at)s, %(lang)s,
         %(word_count)s, %(created_at)s)
    ON CONFLICT (article_id) DO NOTHING
"""

# ---------------------------------------------------------------------------
# Text cleaning
# ---------------------------------------------------------------------------

# Matches any HTML tag: <p>, </div>, <br/>, etc.
_HTML_TAG_RE  = re.compile(r"<[^>]+>")
# Collapse runs of whitespace (spaces, tabs, newlines) to a single space
_WHITESPACE_RE = re.compile(r"\s+")


def clean_body(raw: str | None) -> str | None:
    """
    Strip HTML, decode HTML entities, and normalise whitespace.

    Returns None if the result is empty (so body_clean stays NULL in DB).
    """
    if not raw:
        return None

    text = _HTML_TAG_RE.sub(" ", raw)       # remove HTML tags
    text = html.unescape(text)              # &amp; → &,  &lt; → <, etc.
    text = _WHITESPACE_RE.sub(" ", text)    # collapse whitespace
    text = text.strip()
    return text if text else None


def compute_word_count(body_clean: str | None) -> int | None:
    """Count words in the cleaned body; returns None when body is absent."""
    if not body_clean:
        return None
    return len(body_clean.split())


def clean_headline(raw: str) -> str:
    """Strip HTML and normalise whitespace in the headline."""
    cleaned = _HTML_TAG_RE.sub(" ", raw)
    cleaned = html.unescape(cleaned)
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def normalise_lang(lang: str | None) -> str:
    """Return a safe, lower-cased language code capped at 8 chars."""
    if not lang:
        return "en"
    return lang.strip().lower()[:8]


def ensure_utc(ts: datetime) -> datetime:
    """Attach UTC timezone if the datetime is naive."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_article(raw: dict[str, Any]) -> list[str]:
    """
    Return a list of validation error strings (empty list = valid).

    Rules:
      - headline must be non-empty after cleaning
      - published_at must be a datetime
      - tickers array must be non-empty
    """
    errors: list[str] = []

    headline = clean_headline(raw.get("headline") or "")
    if not headline:
        errors.append("empty headline after cleaning")

    published_at = raw.get("published_at")
    if not isinstance(published_at, datetime):
        errors.append(f"invalid published_at type: {type(published_at)}")

    tickers = raw.get("tickers") or []
    if not tickers:
        errors.append("empty tickers array")

    return errors


# ---------------------------------------------------------------------------
# Dimension resolution
# ---------------------------------------------------------------------------


def resolve_ticker_ids(
    conn: psycopg2.extensions.connection,
    symbols: list[str],
) -> dict[str, int]:
    """
    Return {symbol: ticker_id} for all *symbols* found in dim_ticker.
    Symbols not present (unknown tickers) are silently excluded — the
    caller logs and skips rows whose ticker is unresolvable.
    """
    if not symbols:
        return {}
    with conn.cursor() as cur:
        cur.execute(_RESOLVE_TICKERS_SQL, {"symbols": symbols})
        return {row[0]: row[1] for row in cur.fetchall()}


def resolve_date_ids(
    conn: psycopg2.extensions.connection,
    dates: list[date],
) -> dict[date, int]:
    """Return {trade_date: date_id} for all *dates* found in dim_date."""
    if not dates:
        return {}
    with conn.cursor() as cur:
        cur.execute(_RESOLVE_DATES_SQL, {"dates": dates})
        return {row[0]: row[1] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Core transform
# ---------------------------------------------------------------------------


def fetch_raw_articles(
    conn: psycopg2.extensions.connection,
    target_date: date,
) -> list[dict[str, Any]]:
    """Fetch bronze rows for *target_date* not yet promoted to Silver."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_RAW_SQL, {"target_date": target_date})
        rows = [dict(r) for r in cur.fetchall()]
    log.info("Fetched %d raw articles for %s", len(rows), target_date)
    return rows


def transform(
    raw_articles: list[dict[str, Any]],
    ticker_map: dict[str, int],
    date_map: dict[date, int],
    target_date: date,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Clean, validate, explode, and resolve each raw article into Silver rows.

    Returns:
        silver_rows  – list of dicts ready for INSERT
        counters     – dict of diagnostic counts for logging
    """
    now_utc = datetime.now(timezone.utc)

    counters = {
        "input":              len(raw_articles),
        "validation_failed":  0,
        "unknown_ticker":     0,
        "missing_date_id":    0,
        "duplicate_skipped":  0,
        "output":             0,
    }

    silver_rows: list[dict[str, Any]] = []
    # In-batch dedup: (article_id, ticker_id) — prevents double-inserting the
    # same article for the same ticker when re-running mid-day
    seen: set[tuple[str, int]] = set()

    for raw in raw_articles:
        article_id = str(raw["article_id"])

        # ── 1. Validate ──────────────────────────────────────────────────────
        errors = validate_article(raw)
        if errors:
            log.warning(
                "Skipping article %s – validation failed: %s",
                article_id, "; ".join(errors),
            )
            counters["validation_failed"] += 1
            continue

        # ── 2. Clean fields ───────────────────────────────────────────────────
        headline     = clean_headline(raw["headline"])
        body_clean   = clean_body(raw.get("body"))
        word_count   = compute_word_count(body_clean)
        lang         = normalise_lang(raw.get("lang"))
        published_at = ensure_utc(raw["published_at"])

        # Resolve published_at to a date for dim_date lookup
        publish_date = published_at.date()
        date_id      = date_map.get(publish_date)

        if date_id is None:
            log.warning(
                "Skipping article %s – date %s not found in dim_date",
                article_id, publish_date,
            )
            counters["missing_date_id"] += 1
            continue

        # ── 3. Explode tickers ────────────────────────────────────────────────
        # raw_news_articles.tickers is varchar[] — one article, many tickers.
        # fact_news_articles is one row per (article, ticker).
        raw_tickers: list[str] = raw.get("tickers") or []
        # Normalise: uppercase, strip whitespace, deduplicate within this article
        unique_symbols = list(dict.fromkeys(t.strip().upper() for t in raw_tickers if t.strip()))

        for symbol in unique_symbols:
            ticker_id = ticker_map.get(symbol)
            if ticker_id is None:
                log.debug(
                    "Ticker %s not found in dim_ticker – skipping for article %s",
                    symbol, article_id,
                )
                counters["unknown_ticker"] += 1
                continue

            dedup_key = (article_id, ticker_id)
            if dedup_key in seen:
                log.debug(
                    "Duplicate (article_id=%s, ticker_id=%d) – skipping",
                    article_id, ticker_id,
                )
                counters["duplicate_skipped"] += 1
                continue
            seen.add(dedup_key)

            silver_rows.append({
                "article_id":   article_id,
                "ticker_id":    ticker_id,
                "date_id":      date_id,
                "headline":     headline,
                "body_clean":   body_clean,
                "published_at": published_at,
                "lang":         lang,
                "word_count":   word_count,
                "created_at":   now_utc,
            })
            counters["output"] += 1

    return silver_rows, counters


def insert_silver_rows(
    conn: psycopg2.extensions.connection,
    rows: list[dict[str, Any]],
) -> None:
    """Bulk-insert Silver rows. ON CONFLICT DO NOTHING makes it idempotent."""
    if not rows:
        log.info("No Silver rows to insert")
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _INSERT_SILVER_SQL, rows, page_size=200)
    conn.commit()
    log.info("Inserted %d rows into fact_news_articles", len(rows))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(target_date: date) -> dict[str, Any]:
    """
    Run the Bronze → Silver news transform for *target_date*.

    Returns a summary dict suitable for Airflow XCom or logging.
    """
    conn = get_connection()
    log.info("Connected to database | target_date=%s", target_date)

    # ── Fetch raw bronze articles ─────────────────────────────────────────────
    raw_articles = fetch_raw_articles(conn, target_date)
    if not raw_articles:
        log.info("No new raw articles to process for %s", target_date)
        conn.close()
        return {
            "date":            str(target_date),
            "raw_fetched":     0,
            "silver_inserted": 0,
            "skipped": {
                "validation_failed": 0,
                "unknown_ticker":    0,
                "missing_date_id":   0,
                "duplicate":         0,
            },
        }

    # ── Pre-resolve dimensions (one DB round-trip each) ───────────────────────
    # Collect all unique symbols across all articles
    all_symbols: list[str] = list(dict.fromkeys(
        sym.strip().upper()
        for row in raw_articles
        for sym in (row.get("tickers") or [])
        if sym.strip()
    ))

    # Collect all unique publish dates (usually just one, but can span midnight)
    all_dates: list[date] = list(dict.fromkeys(
        row["published_at"].date()
        for row in raw_articles
        if isinstance(row.get("published_at"), datetime)
    ))

    ticker_map = resolve_ticker_ids(conn, all_symbols)
    date_map   = resolve_date_ids(conn, all_dates)

    log.info(
        "Resolved %d/%d ticker symbols | %d/%d dates",
        len(ticker_map), len(all_symbols),
        len(date_map),   len(all_dates),
    )

    unresolved_tickers = set(all_symbols) - set(ticker_map)
    if unresolved_tickers:
        log.warning(
            "Tickers not found in dim_ticker (articles will be skipped for these): %s",
            ", ".join(sorted(unresolved_tickers)),
        )

    unresolved_dates = set(all_dates) - set(date_map)
    if unresolved_dates:
        log.warning(
            "Dates not found in dim_date (articles will be skipped): %s",
            ", ".join(str(d) for d in sorted(unresolved_dates)),
        )

    # ── Transform ─────────────────────────────────────────────────────────────
    silver_rows, counters = transform(raw_articles, ticker_map, date_map, target_date)

    log.info(
        "Transform complete | input=%d validation_failed=%d unknown_ticker=%d "
        "missing_date=%d duplicate_skipped=%d output=%d",
        counters["input"],
        counters["validation_failed"],
        counters["unknown_ticker"],
        counters["missing_date_id"],
        counters["duplicate_skipped"],
        counters["output"],
    )

    # ── Insert ────────────────────────────────────────────────────────────────
    insert_silver_rows(conn, silver_rows)
    conn.close()

    return {
        "date":            str(target_date),
        "raw_fetched":     counters["input"],
        "silver_inserted": counters["output"],
        "skipped": {
            "validation_failed": counters["validation_failed"],
            "unknown_ticker":    counters["unknown_ticker"],
            "missing_date_id":   counters["missing_date_id"],
            "duplicate":         counters["duplicate_skipped"],
        },
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bronze → Silver news transform: clean, explode, and promote articles",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target publish date YYYY-MM-DD. Defaults to yesterday.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid date '%s' – expected YYYY-MM-DD", args.date)
            sys.exit(1)
    else:
        target_date = date.today() - timedelta(days=1)

    log.info("Starting Bronze → Silver news transform | date=%s", target_date)
    summary = run(target_date)

    log.info(
        "Done | raw_fetched=%d silver_inserted=%d skipped=%s",
        summary["raw_fetched"],
        summary["silver_inserted"],
        summary.get("skipped", {}),
    )


if __name__ == "__main__":
    main()