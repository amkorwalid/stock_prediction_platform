"""
base_operator.py
~~~~~~~~~~~~~~~~
Abstract base for all Stock Prediction Platform operators.

Responsibilities
----------------
- Opens / closes a managed psycopg2 connection (from the Airflow
  'postgres_stock' connection).
- Records every run in metadata.pipeline_runs (start → running →
  success | failed).
- Writes WARNING+ log lines to audit.system_logs via a custom
  logging Handler so every operator gets structured DB logging
  for free without extra code.
- Subclasses only implement execute_pipeline().
"""

from __future__ import annotations

import logging
import time
import traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.context import Context


# ──────────────────────────────────────────────────────────────────────────────
# DB-backed log handler
# ──────────────────────────────────────────────────────────────────────────────

class _DBLogHandler(logging.Handler):
    """Writes WARNING+ records to audit.system_logs via a shared connection."""

    def __init__(self, conn: psycopg2.extensions.connection, component: str) -> None:
        super().__init__(level=logging.WARNING)
        self._conn = conn
        self._component = component

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            tb = record.exc_text or None
            meta: dict[str, Any] = {
                "filename": record.filename,
                "lineno": record.lineno,
                "funcName": record.funcName,
            }
            with self._conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO audit.system_logs
                        (log_level, component, module, message, stack_trace, metadata, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        record.levelname,
                        self._component,
                        record.module,
                        msg,
                        tb,
                        psycopg2.extras.Json(meta),
                        datetime.now(timezone.utc),
                    ),
                )
            # Autocommit is ON for the shared connection so each INSERT lands
            # immediately, even if the operator later raises an exception.
        except Exception:  # noqa: BLE001
            self.handleError(record)


# ──────────────────────────────────────────────────────────────────────────────
# Abstract base operator
# ──────────────────────────────────────────────────────────────────────────────

class StockBaseOperator(BaseOperator, ABC):
    """
    Base class for all pipeline operators.

    Parameters
    ----------
    postgres_conn_id:
        Airflow connection ID for the stock platform database.
    pipeline_name:
        Must match a row in metadata.etl_pipelines.pipeline_name.
        If the row doesn't exist yet it will be created automatically.
    """

    # Subclasses can declare which Airflow Variables / env vars they need;
    # the base class logs them on startup so misconfigurations are obvious.
    required_variables: list[str] = []

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_stock",
        pipeline_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.pipeline_name = pipeline_name
        self._pipeline_id: int | None = None
        self._run_id: int | None = None

    # ── public entry-point (called by Airflow) ────────────────────────────────

    def execute(self, context: Context) -> Any:
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = hook.get_conn()
        conn.autocommit = True

        log = logging.getLogger(f"stock_pipeline.{self.pipeline_name}")
        db_handler = _DBLogHandler(conn, component=self.pipeline_name)
        log.addHandler(db_handler)

        try:
            self._ensure_pipeline_row(conn)
            run_id = self._open_run(conn, context)
            t0 = time.perf_counter()

            result = self.execute_pipeline(context, conn, log)

            elapsed = int(time.perf_counter() - t0)
            records = result if isinstance(result, int) else None
            self._close_run(conn, run_id, "success", elapsed, records)
            self._update_pipeline_last_run(conn, "success", elapsed, records)
            return result

        except Exception as exc:
            tb_str = traceback.format_exc()
            log.error("Pipeline failed: %s", exc, exc_info=True)
            if self._run_id:
                self._close_run(
                    conn,
                    self._run_id,
                    "failed",
                    error_message=str(exc),
                    error_stack_trace=tb_str,
                )
                self._update_pipeline_last_run(conn, "failed")
            raise
        finally:
            log.removeHandler(db_handler)
            conn.close()

    # ── abstract method for subclasses ───────────────────────────────────────

    @abstractmethod
    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int | None:
        """
        Implement the operator logic here.

        Parameters
        ----------
        context : Airflow task context dict
        conn    : live psycopg2 connection (autocommit=True)
        log     : logger wired to both stdout and audit.system_logs

        Returns
        -------
        int | None
            Number of records processed, or None if not applicable.
            Returned ints are stored in pipeline_runs.records_written.
        """

    # ── metadata helpers ──────────────────────────────────────────────────────

    def _ensure_pipeline_row(self, conn: psycopg2.extensions.connection) -> None:
        """Upsert a row in metadata.etl_pipelines."""
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.etl_pipelines (pipeline_name, status, is_active)
                VALUES (%s, 'idle', true)
                ON CONFLICT (pipeline_name) DO NOTHING
                RETURNING pipeline_id
                """,
                (self.pipeline_name,),
            )
            row = cur.fetchone()
            if row:
                self._pipeline_id = row[0]
            else:
                cur.execute(
                    "SELECT pipeline_id FROM metadata.etl_pipelines WHERE pipeline_name = %s",
                    (self.pipeline_name,),
                )
                self._pipeline_id = cur.fetchone()[0]

    def _open_run(
        self, conn: psycopg2.extensions.connection, context: Context
    ) -> int:
        triggered_by = "scheduler"
        triggered_by_user = None
        if context.get("dag_run") and context["dag_run"].run_type == "manual":
            triggered_by = "manual"
            triggered_by_user = context["dag_run"].conf.get("user", "airflow")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.pipeline_runs
                    (pipeline_id, run_start, status, triggered_by, triggered_by_user)
                VALUES (%s, %s, 'running', %s, %s)
                RETURNING run_id
                """,
                (
                    self._pipeline_id,
                    datetime.now(timezone.utc),
                    triggered_by,
                    triggered_by_user,
                ),
            )
            self._run_id = cur.fetchone()[0]

        # Mark the pipeline itself as running
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.etl_pipelines
                SET status = 'running', last_run_start = %s, updated_at = now()
                WHERE pipeline_id = %s
                """,
                (datetime.now(timezone.utc), self._pipeline_id),
            )
        return self._run_id

    def _close_run(
        self,
        conn: psycopg2.extensions.connection,
        run_id: int,
        status: str,
        duration_seconds: int | None = None,
        records_written: int | None = None,
        error_message: str | None = None,
        error_stack_trace: str | None = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.pipeline_runs
                SET run_end              = %s,
                    status               = %s,
                    records_written      = COALESCE(%s, records_written),
                    error_message        = %s,
                    error_stack_trace    = %s
                WHERE run_id = %s
                """,
                (
                    datetime.now(timezone.utc),
                    status,
                    records_written,
                    error_message,
                    error_stack_trace,
                    run_id,
                ),
            )

    def _update_pipeline_last_run(
        self,
        conn: psycopg2.extensions.connection,
        status: str,
        duration_seconds: int | None = None,
        records: int | None = None,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.etl_pipelines
                SET status                   = 'idle',
                    last_run_end             = now(),
                    last_run_status          = %s,
                    last_run_duration_seconds = %s,
                    last_run_records_processed = %s,
                    updated_at               = now()
                WHERE pipeline_id = %s
                """,
                (status, duration_seconds, records, self._pipeline_id),
            )

    # ── shared context manager for sub-transactions ───────────────────────────

    @contextmanager
    def transaction(self, conn: psycopg2.extensions.connection):
        """
        Temporarily disable autocommit for a block that needs atomicity.

        Usage::

            with self.transaction(conn):
                cur.execute(...)
                cur.execute(...)   # both committed or both rolled back
        """
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = True
