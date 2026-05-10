"""
feature_engineering_operator.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Calls the standalone gold_transform.py script to rebuild the Gold layer
with all ML-ready features.

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


class FeatureEngineeringOperator(StockBaseOperator):
    """
    Calls the standalone gold_transform.py script to rebuild Gold layer.

    The script performs a full-snapshot rebuild of:
    - feat_technical: technical indicators (SMAs, EMAs, RSI, MACD, Bollinger Bands, ATR, OBV, VWAP)
    - feat_price_dynamics: price dynamics features (returns, lags, momentum, volatility, volume)

    All features are pre-shifted at write time to prevent leakage during training.

    Parameters
    ----------
    script_path :
        Path to gold_transform.py relative to working directory.
        Default: "scripts/etl/gold_transform.py"
    """

    ui_color = "#FAC775"   # amber-100 — Gold layer

    def __init__(
        self,
        *,
        script_path: str = "scripts/etl/gold_transform.py",
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_name="feature_engineering", **kwargs)
        self.script_path = script_path

    # ── operator logic ────────────────────────────────────────────────────────

    def execute_pipeline(
        self,
        context: Context,
        conn: psycopg2.extensions.connection,
        log: logging.Logger,
    ) -> int:
        log.info("Starting Gold transform via script: %s", self.script_path)

        env = os.environ.copy()

        try:
            result = subprocess.run(
                ["python", self.script_path],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour max
                check=True,
                env=env,
            )
            log.info("Script stdout:\n%s", result.stdout)
            if result.stderr:
                log.warning("Script stderr:\n%s", result.stderr)

            # Parse row count from last log line if possible
            lines = result.stdout.strip().split("\n")
            records_written = self._parse_record_count(lines, log)

            log.info("Gold transform complete. Records written: %s", records_written or "unknown")
            return records_written or 0

        except subprocess.CalledProcessError as exc:
            log.error("Script failed with exit code %d", exc.returncode)
            log.error("stdout:\n%s", exc.stdout)
            log.error("stderr:\n%s", exc.stderr)
            raise RuntimeError(f"gold_transform.py failed: {exc.stderr}") from exc
        except subprocess.TimeoutExpired:
            log.error("Script timeout after 3600 seconds")
            raise RuntimeError("gold_transform.py timeout") from None
        except Exception as exc:
            log.error("Unexpected error calling script: %s", exc)
            raise

    # ── Helper ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_record_count(log_lines: list[str], log: logging.Logger) -> int | None:
        """Attempt to extract record count from script output logs."""
        total = 0
        for line in reversed(log_lines):
            # Look for patterns like "Rebuilt feat_technical with 31015 rows"
            # or "Rebuilt feat_price_dynamics with 31015 rows"
            if "rows" in line and "Rebuilt" in line:
                parts = line.split()
                try:
                    # Find the last numeric value before "rows"
                    for i, part in enumerate(parts):
                        if part == "rows" and i > 0:
                            total += int(parts[i - 1])
                except (ValueError, IndexError):
                    pass
        return total if total > 0 else None
