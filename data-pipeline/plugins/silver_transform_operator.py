"""
silver_transform_operator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Calls the standalone silver_transform.py script to rebuild the Silver layer
from the deduplicated Bronze layer.

The operator:
- Calls the standalone script (no CLI args — full snapshot rebuild)
- Tracks pipeline execution in metadata.pipeline_runs
- Logs to audit.system_logs for all WARNING+ messages
- Returns the number of rows written for metadata tracking
"""

from __future__ import annotations

import logging
import os
import subprocess
from typing import Any

import psycopg2
from airflow.utils.context import Context

from plugins.base_operator import StockBaseOperator


class SilverTransformOperator(StockBaseOperator):
    """
    Calls the standalone silver_transform.py script to rebuild Silver layer.

    The script performs:
    - Full snapshot rebuild of fact_daily_prices from deduplicated raw_ohlcv_prices
    - Upserting of dimension tables (dim_ticker, dim_date)
    - Per-ticker forward-fill imputation for NaN values

    Parameters
    ----------
    script_path :
        Path to silver_transform.py relative to Airflow DAGS_FOLDER.
        Default: "../scripts/etl/silver_transform.py"
    """

    ui_color = "#B5D4F4"   # blue-100 — Silver layer

    def __init__(
        self,
        *,
        script_path: str = "../scripts/etl/silver_transform.py",
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="silver_transform", **kwargs)
        self.script_path = script_path

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        log.info("Starting Silver transform via script: %s", self.script_path)

        try:
            result = subprocess.run(
                ["python", self.script_path],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour max
                check=True,
            )
            log.info("Script stdout:\n%s", result.stdout)
            if result.stderr:
                log.warning("Script stderr:\n%s", result.stderr)

            # Parse row count from last log line if possible
            lines = result.stdout.strip().split("\n")
            records_written = self._parse_record_count(lines, log)

            log.info("Silver transform complete. Records written: %s", records_written or "unknown")
            return records_written or 0

        except subprocess.CalledProcessError as exc:
            log.error("Script failed with exit code %d", exc.returncode)
            log.error("stdout:\n%s", exc.stdout)
            log.error("stderr:\n%s", exc.stderr)
            raise RuntimeError(f"silver_transform.py failed: {exc.stderr}") from exc
        except subprocess.TimeoutExpired:
            log.error("Script timeout after 3600 seconds")
            raise RuntimeError("silver_transform.py timeout") from None
        except Exception as exc:
            log.error("Unexpected error calling script: %s", exc)
            raise

    # ── Helper ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_record_count(log_lines: list[str], log: logging.Logger) -> int | None:
        """Attempt to extract record count from script output logs."""
        for line in reversed(log_lines):
            # Look for patterns like "Rebuilt fact_daily_prices with 31015 rows"
            if "rows" in line and "Rebuilt" in line:
                parts = line.split()
                try:
                    # Find the last numeric value before "rows"
                    for i, part in enumerate(parts):
                        if part == "rows" and i > 0:
                            return int(parts[i - 1])
                except (ValueError, IndexError):
                    pass
        return None
