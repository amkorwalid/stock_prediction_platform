"""
scripts/preprocessing/sentiment.py
------------------------------------
Reads unprocessed articles from data_warehouse.fact_news_articles,
calls the DeepSeek API to analyse sentiment per ticker, and writes
one aggregated row per (ticker, date) into data_warehouse.feat_news_sentiment.

Usage (standalone):
    python sentiment.py --date 2025-05-09

Usage (Airflow PythonOperator):
    from scripts.preprocessing.sentiment import run
    run(target_date=date(2025, 5, 9))

.env file:
    DEEPSEEK_API_KEY=sk-...
    DEEPSEEK_BASE_URL=https://api.deepseek.com   # or your self-hosted URL
    DB_HOST=your-db-private-ip
    DB_PORT=5432
    DB_NAME=stockdb
    ETL_USER_USERNAME=etl_user
    ETL_USER_PASSWORD=secret

DeepSeek response contract (JSON returned by the model):
    {
      "sentiment_label":  "UP" | "DOWN" | "CONSTANT",
      "sentiment_score":  float,   // -1.0 (bearish) … +1.0 (bullish)
      "relevance":        int,     // 0-100, model confidence the article is stock-relevant
      "analysis_summary": str      // ≤3 sentence plain-English rationale
    }
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
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
log = logging.getLogger("sentiment")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL    = "deepseek-chat"
REQUEST_DELAY     = 1.0   # seconds between DeepSeek calls (adjust to your rate limit)
MAX_BODY_CHARS    = 1500  # truncate long articles before sending to the LLM

# System prompt — instructs DeepSeek to return strict JSON only
_SYSTEM_PROMPT = """
You are a financial news analyst specialising in stock market sentiment.

Given a news article about a specific stock ticker, you must respond ONLY with
a valid JSON object and nothing else — no markdown, no explanation outside the JSON.

The JSON must have exactly these four keys:

  "sentiment_label":  one of "UP", "DOWN", or "CONSTANT"
                      (your prediction of the stock price direction after this news)
  "sentiment_score":  a float in [-1.0, 1.0]
                      (-1.0 = strongly bearish, 0.0 = neutral, +1.0 = strongly bullish)
  "relevance":        an integer in [0, 100]
                      (how relevant/impactful this article is to the specific stock;
                       0 = not relevant, 100 = extremely relevant)
  "analysis_summary": a plain-English string of at most 3 sentences explaining
                      your reasoning

Example of a valid response:
{
  "sentiment_label": "UP",
  "sentiment_score": 0.72,
  "relevance": 85,
  "analysis_summary": "Apple beat earnings expectations by 12%, driven by strong iPhone 16 sales. Services revenue hit an all-time high. Analysts are expected to revise price targets upward."
}
""".strip()

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


# Query: fetch all (ticker_id, date_id, article_id, symbol, headline, body_clean)
# for a given date that have not yet been processed into feat_news_sentiment.
_FETCH_ARTICLES_SQL = """
    SELECT
        fna.article_id,
        fna.ticker_id,
        fna.date_id,
        dt.symbol        AS ticker_symbol,
        fna.headline,
        fna.body_clean
    FROM data_warehouse.fact_news_articles  fna
    JOIN data_warehouse.dim_ticker          dt  ON dt.ticker_id = fna.ticker_id
    JOIN data_warehouse.dim_date            dd  ON dd.date_id   = fna.date_id
    WHERE dd.trade_date = %(trade_date)s
      AND NOT EXISTS (
          SELECT 1
          FROM data_warehouse.feat_news_sentiment fns
          WHERE fns.article_id::text = fna.article_id::text
      )
    ORDER BY fna.ticker_id, fna.article_id
"""

_INSERT_SENTIMENT_SQL = """
    INSERT INTO data_warehouse.feat_news_sentiment
        (ticker_id, date_id, article_id,
         analysis_summary, sentiment_score, sentiment_label,
         relevance, computed_at)
    VALUES
        (%(ticker_id)s, %(date_id)s, %(article_id)s,
         %(analysis_summary)s, %(sentiment_score)s, %(sentiment_label)s,
         %(relevance)s, %(computed_at)s)
    ON CONFLICT (ticker_id, date_id) DO UPDATE SET
        analysis_summary = EXCLUDED.analysis_summary,
        sentiment_score  = EXCLUDED.sentiment_score,
        sentiment_label  = EXCLUDED.sentiment_label,
        relevance        = EXCLUDED.relevance,
        computed_at      = EXCLUDED.computed_at
"""


def fetch_unprocessed_articles(
    conn: psycopg2.extensions.connection,
    trade_date: date,
) -> list[dict[str, Any]]:
    """Return all unprocessed fact_news_articles rows for *trade_date*."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_FETCH_ARTICLES_SQL, {"trade_date": trade_date})
        rows = cur.fetchall()
    log.info("Found %d unprocessed articles for %s", len(rows), trade_date)
    return [dict(r) for r in rows]


def insert_sentiment_rows(
    conn: psycopg2.extensions.connection,
    rows: list[dict[str, Any]],
) -> None:
    """Upsert sentiment rows. ON CONFLICT updates so re-runs refine results."""
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _INSERT_SENTIMENT_SQL, rows, page_size=50)
    conn.commit()
    log.info("Upserted %d rows into feat_news_sentiment", len(rows))


# ---------------------------------------------------------------------------
# DeepSeek API
# ---------------------------------------------------------------------------


def _build_user_prompt(ticker: str, headline: str, body: str | None) -> str:
    """Compose the per-article prompt sent to DeepSeek."""
    body_text = (body or "").strip()
    if len(body_text) > MAX_BODY_CHARS:
        body_text = body_text[:MAX_BODY_CHARS] + "… [truncated]"

    parts = [
        f"Ticker: {ticker}",
        f"Headline: {headline}",
    ]
    if body_text:
        parts.append(f"Article body:\n{body_text}")

    return "\n\n".join(parts)


def call_deepseek(
    api_key: str,
    ticker: str,
    headline: str,
    body: str | None,
) -> dict[str, Any]:
    """
    Send one article to DeepSeek and return the parsed JSON response.

    Raises ValueError if the model returns malformed JSON or missing keys.
    Raises requests.HTTPError on HTTP failures.
    """
    url = f"{DEEPSEEK_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "temperature": 0.0,   # deterministic output for reproducibility
        "max_tokens": 300,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": _build_user_prompt(ticker, headline, body)},
        ],
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()

    raw_content = resp.json()["choices"][0]["message"]["content"]

    try:
        result = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"DeepSeek returned non-JSON content: {raw_content!r}") from exc

    _validate_deepseek_response(result)
    return result


def _validate_deepseek_response(result: dict) -> None:
    """Raise ValueError if required keys are missing or values out of range."""
    required = {"sentiment_label", "sentiment_score", "relevance", "analysis_summary"}
    missing = required - result.keys()
    if missing:
        raise ValueError(f"DeepSeek response missing keys: {missing}. Got: {result}")

    if result["sentiment_label"] not in ("UP", "DOWN", "CONSTANT"):
        raise ValueError(f"Invalid sentiment_label: {result['sentiment_label']!r}")

    score = float(result["sentiment_score"])
    if not (-1.0 <= score <= 1.0):
        raise ValueError(f"sentiment_score out of range [-1, 1]: {score}")

    relevance = int(result["relevance"])
    if not (0 <= relevance <= 100):
        raise ValueError(f"relevance out of range [0, 100]: {relevance}")


# ---------------------------------------------------------------------------
# Aggregation  (multiple articles → one row per ticker+date)
# ---------------------------------------------------------------------------


def aggregate_ticker_sentiments(
    article_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Aggregate multiple per-article sentiment results for the same (ticker, date)
    into a single feat_news_sentiment row.

    Strategy:
      - sentiment_score  : mean of all article scores (weighted equally)
      - sentiment_label  : derived from the aggregated score
                           (> 0.05 → UP, < -0.05 → DOWN, else CONSTANT)
      - relevance        : mean relevance score, rounded to int
      - analysis_summary : combined from the most relevant article's summary
                           plus a count note if there were multiple articles
    """
    scores     = [r["sentiment_score"]  for r in article_results]
    relevances = [r["relevance"]        for r in article_results]

    mean_score     = sum(scores) / len(scores)
    mean_relevance = round(sum(relevances) / len(relevances))

    if mean_score > 0.05:
        label = "UP"
    elif mean_score < -0.05:
        label = "DOWN"
    else:
        label = "CONSTANT"

    # Pick the most relevant article's summary as the representative text
    best = max(article_results, key=lambda r: r["relevance"])
    summary = best["analysis_summary"]
    if len(article_results) > 1:
        summary += f" [Aggregated from {len(article_results)} articles]"

    return {
        "sentiment_score":  round(mean_score, 4),
        "sentiment_label":  label,
        "relevance":        mean_relevance,
        "analysis_summary": summary,
        # article_id is set to the most-relevant article's id
        "article_id":       best["article_id"],
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run(target_date: date) -> dict[str, Any]:
    """
    Full pipeline for *target_date*:
      1. Fetch unprocessed articles from fact_news_articles
      2. Call DeepSeek for each article
      3. Aggregate per (ticker, date)
      4. Upsert into feat_news_sentiment

    Returns a summary dict for logging / XCom.
    """
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise EnvironmentError("DEEPSEEK_API_KEY is not set")

    conn = get_connection()
    log.info("Database connection established")

    articles = fetch_unprocessed_articles(conn, target_date)
    if not articles:
        log.info("No unprocessed articles for %s – nothing to do", target_date)
        conn.close()
        return {"date": str(target_date), "articles_processed": 0, "rows_inserted": 0}

    # Group articles by (ticker_id, date_id) for aggregation
    groups: dict[tuple[int, int], list[dict]] = {}
    for art in articles:
        key = (art["ticker_id"], art["date_id"])
        groups.setdefault(key, []).append(art)

    sentiment_rows: list[dict[str, Any]] = []
    errors = 0

    total_articles = len(articles)
    processed = 0

    for (ticker_id, date_id), group_articles in groups.items():
        ticker_symbol = group_articles[0]["ticker_symbol"]
        log.info(
            "Processing ticker=%s | %d article(s) for date_id=%d",
            ticker_symbol, len(group_articles), date_id,
        )

        article_results: list[dict[str, Any]] = []

        for art in group_articles:
            if processed > 0:
                time.sleep(REQUEST_DELAY)

            try:
                result = call_deepseek(
                    api_key,
                    ticker=ticker_symbol,
                    headline=art["headline"],
                    body=art.get("body_clean"),
                )
                result["article_id"] = str(art["article_id"])
                article_results.append(result)
                log.info(
                    "  article %s → label=%s score=%.3f relevance=%d",
                    art["article_id"],
                    result["sentiment_label"],
                    result["sentiment_score"],
                    result["relevance"],
                )
            except Exception as exc:
                log.error(
                    "  Failed to analyse article %s for %s: %s",
                    art["article_id"], ticker_symbol, exc,
                )
                errors += 1

            processed += 1

        if not article_results:
            log.warning("No successful analyses for ticker=%s — skipping", ticker_symbol)
            continue

        # Aggregate multiple articles into one DB row
        aggregated = aggregate_ticker_sentiments(article_results)

        sentiment_rows.append({
            "ticker_id":        ticker_id,
            "date_id":          date_id,
            "article_id":       aggregated["article_id"],
            "analysis_summary": aggregated["analysis_summary"],
            "sentiment_score":  Decimal(str(aggregated["sentiment_score"])),
            "sentiment_label":  aggregated["sentiment_label"],
            "relevance":        aggregated["relevance"],
            "computed_at":      datetime.now(timezone.utc),
        })

    insert_sentiment_rows(conn, sentiment_rows)
    conn.close()

    summary = {
        "date":               str(target_date),
        "articles_processed": processed,
        "rows_inserted":      len(sentiment_rows),
        "errors":             errors,
    }
    log.info("Pipeline complete: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyse news sentiment via DeepSeek and store in feat_news_sentiment",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target trade date YYYY-MM-DD. Defaults to yesterday.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()

    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid date format '%s' – expected YYYY-MM-DD", args.date)
            sys.exit(1)
    else:
        target_date = date.today() - timedelta(days=1)

    log.info("Starting sentiment analysis for %s", target_date)

    try:
        summary = run(target_date)
    except EnvironmentError as exc:
        log.error("%s", exc)
        sys.exit(1)

    log.info(
        "Done | articles=%d rows_inserted=%d errors=%d",
        summary["articles_processed"],
        summary["rows_inserted"],
        summary["errors"],
    )


if __name__ == "__main__":
    main()