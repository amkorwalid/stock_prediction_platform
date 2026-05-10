"""
sentiment_operator.py
~~~~~~~~~~~~~~~~~~~~~
Sends Silver news articles to an LLM API (OpenAI-compatible endpoint)
for sentiment analysis and writes results to
data_warehouse.feat_news_sentiment (Gold layer).

Each run processes articles for a single execution date that have not
yet been scored (idempotent: re-running skips already-scored articles).

Airflow Variables expected
--------------------------
OPENAI_API_KEY    API key for the LLM provider (OpenAI or compatible).
LLM_MODEL        Model name, e.g. "gpt-4o-mini". Default: "gpt-4o-mini".
LLM_BATCH_SIZE   Articles per LLM request. Default: 5.

Airflow Connections expected
----------------------------
postgres_stock   PostgreSQL connection to the platform database.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from airflow.models import Variable
from airflow.utils.context import Context

from plugins.base_operator import StockBaseOperator

_OPENAI_URL = "https://api.openai.com/v1/chat/completions"
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF_SEC = 5

_SYSTEM_PROMPT = """You are a financial news sentiment analyst.
For each article you receive, return ONLY a valid JSON object with these fields:
  sentiment_score  : float from -1.0 (very bearish) to 1.0 (very bullish)
  sentiment_label  : one of "UP", "DOWN", or "CONSTANT"
  relevance        : integer 0-100 (how relevant this article is to the stock price)
  analysis_summary : 1-2 sentence summary of the article's market implication

Return ONLY the JSON object with no markdown, no code fences, no extra text."""


class SentimentOperator(StockBaseOperator):
    """
    Scores news articles using an LLM and populates feat_news_sentiment.

    Parameters
    ----------
    execution_date_override :
        ISO date string for manual backfill runs.
    batch_size :
        Number of articles per LLM call. Overrides the LLM_BATCH_SIZE Variable.
    request_delay_sec :
        Sleep between LLM API calls to stay within rate limits. Default: 1.0.
    """

    ui_color = "#FAC775"   # amber-100 — Gold layer

    required_variables = ["OPENAI_API_KEY"]

    def __init__(
        self,
        *,
        execution_date_override: str | None = None,
        batch_size: int | None = None,
        request_delay_sec: float = 1.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="sentiment_analysis", **kwargs)
        self._date_override = execution_date_override
        self._batch_size = batch_size
        self.request_delay_sec = request_delay_sec

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        exec_date = self._resolve_date(context)
        api_key = Variable.get("OPENAI_API_KEY")
        model = Variable.get("LLM_MODEL", default_var="gpt-4o-mini")
        batch_size = self._batch_size or int(Variable.get("LLM_BATCH_SIZE", default_var="5"))

        articles = self._load_unscored_articles(conn, exec_date, log)
        if not articles:
            log.info("No unscored articles for %s. Nothing to do.", exec_date)
            return 0

        log.info(
            "Scoring %d article(s) for %s using model '%s' (batch_size=%d)",
            len(articles),
            exec_date,
            model,
            batch_size,
        )

        total_written = 0
        # Process in batches to reduce API calls
        for i in range(0, len(articles), batch_size):
            batch = articles[i : i + batch_size]
            scored = self._score_batch(batch, api_key, model, log)
            written = self._upsert_sentiment(conn, scored, log)
            total_written += written

            if i + batch_size < len(articles):
                time.sleep(self.request_delay_sec)

        log.info("Sentiment analysis complete. Rows written: %d", total_written)
        return total_written

    # ── data loading ──────────────────────────────────────────────────────────

    @staticmethod
    def _load_unscored_articles(
        conn: psycopg2.extensions.connection,
        exec_date: date,
        log: logging.Logger,
    ) -> list[dict]:
        """
        Return Silver articles for exec_date that have no matching row
        in feat_news_sentiment yet.
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    fna.article_id,
                    fna.ticker_id,
                    fna.date_id,
                    fna.headline,
                    fna.body_clean
                FROM data_warehouse.fact_news_articles fna
                LEFT JOIN data_warehouse.feat_news_sentiment fns
                    ON fns.article_id = fna.article_id::varchar
                    AND fns.ticker_id = fna.ticker_id
                WHERE fna.date_id = (
                    SELECT date_id FROM data_warehouse.dim_date WHERE trade_date = %s
                )
                AND fns.id IS NULL
                ORDER BY fna.published_at
                """,
                (exec_date,),
            )
            rows = cur.fetchall()

        return [dict(r) for r in rows]

    # ── LLM scoring ───────────────────────────────────────────────────────────

    def _score_batch(
        self,
        articles: list[dict],
        api_key: str,
        model: str,
        log: logging.Logger,
    ) -> list[dict]:
        """
        Send a batch of articles to the LLM. Each article is scored
        independently via a separate message in the conversation.
        Returns the input dicts enriched with sentiment fields.
        """
        results = []
        for article in articles:
            text = f"Headline: {article['headline']}\n\nBody: {article.get('body_clean') or ''}"
            sentiment = self._call_llm(text, api_key, model, log)
            results.append({**article, **sentiment})
        return results

    def _call_llm(
        self,
        article_text: str,
        api_key: str,
        model: str,
        log: logging.Logger,
    ) -> dict:
        """Call the LLM API with retry logic. Returns a dict with sentiment fields."""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "temperature": 0,
            "max_tokens": 300,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": article_text},
            ],
        }

        last_exc: Exception | None = None
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                resp = requests.post(_OPENAI_URL, headers=headers, json=payload, timeout=30)
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"].strip()
                parsed = json.loads(content)
                return {
                    "sentiment_score": float(parsed.get("sentiment_score", 0)),
                    "sentiment_label": str(parsed.get("sentiment_label", "CONSTANT")).upper(),
                    "relevance": int(parsed.get("relevance", 50)),
                    "analysis_summary": str(parsed.get("analysis_summary", "")),
                }
            except Exception as exc:
                last_exc = exc
                log.warning("LLM call attempt %d/%d failed: %s", attempt, _RETRY_ATTEMPTS, exc)
                if attempt < _RETRY_ATTEMPTS:
                    time.sleep(_RETRY_BACKOFF_SEC * attempt)

        log.error("All LLM retry attempts exhausted. Storing neutral fallback.")
        return {
            "sentiment_score": 0.0,
            "sentiment_label": "CONSTANT",
            "relevance": 0,
            "analysis_summary": f"LLM scoring failed: {last_exc}",
        }

    # ── DB write ──────────────────────────────────────────────────────────────

    @staticmethod
    def _upsert_sentiment(
        conn: psycopg2.extensions.connection,
        scored: list[dict],
        log: logging.Logger,
    ) -> int:
        if not scored:
            return 0

        sql = """
            INSERT INTO data_warehouse.feat_news_sentiment
                (ticker_id, date_id, article_id, analysis_summary,
                 sentiment_score, sentiment_label, relevance, computed_at)
            VALUES
                (%(ticker_id)s, %(date_id)s, %(article_id)s, %(analysis_summary)s,
                 %(sentiment_score)s, %(sentiment_label)s, %(relevance)s, %(computed_at)s)
            ON CONFLICT (ticker_id, date_id)
            DO UPDATE SET
                analysis_summary = EXCLUDED.analysis_summary,
                sentiment_score  = EXCLUDED.sentiment_score,
                sentiment_label  = EXCLUDED.sentiment_label,
                relevance        = EXCLUDED.relevance,
                computed_at      = EXCLUDED.computed_at
        """
        now = datetime.now(timezone.utc)
        rows = [{**s, "computed_at": now} for s in scored]

        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
            return len(rows)
        except Exception as exc:
            log.error("Failed to upsert sentiment rows: %s", exc)
            raise

    # ── utilities ─────────────────────────────────────────────────────────────

    def _resolve_date(self, context: Context) -> date:
        if self._date_override:
            return datetime.strptime(self._date_override, "%Y-%m-%d").date()
        ds = context.get("ds")
        if ds:
            return datetime.strptime(ds, "%Y-%m-%d").date()
        return datetime.now(timezone.utc).date()
