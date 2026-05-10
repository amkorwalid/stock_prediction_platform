"""
stock_data_pipeline.py
~~~~~~~~~~~~~~~~~~~~~~
Airflow DAG for the full stock prediction data pipeline.

Pipeline flow
-------------
Bronze:
  fetch_ohlcv -> fetch_news

Silver:
  silver_transform

Gold:
  sentiment_analysis -> feature_engineering

Platinum:
  model_training

The Silver and Gold transform operators call the standalone scripts in
data-pipeline/scripts/etl so the scripts remain the single source of truth.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.feature_engineering_operator import FeatureEngineeringOperator
from plugins.fetch_news_operator import FetchNewsOperator
from plugins.fetch_ohlcv_operator import FetchOHLCVOperator
from plugins.model_training_operator import ModelTrainingOperator
from plugins.sentiment_operator import SentimentOperator
from plugins.silver_transform_operator import SilverTransformOperator


PIPELINE_ROOT = Path(__file__).resolve().parents[1]
SILVER_SCRIPT = str(PIPELINE_ROOT / "scripts" / "etl" / "silver_transform.py")
GOLD_SCRIPT = str(PIPELINE_ROOT / "scripts" / "etl" / "gold_transform.py")


default_args = {
    "owner": "stock-pipeline",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="stock_data_pipeline",
    description="End-to-end stock data pipeline from market/news ingestion to training",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stock", "etl", "ml", "pipeline"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    fetch_ohlcv = FetchOHLCVOperator(task_id="fetch_ohlcv")
    fetch_news = FetchNewsOperator(task_id="fetch_news")

    silver_transform = SilverTransformOperator(
        task_id="silver_transform",
        script_path=SILVER_SCRIPT,
    )

    sentiment_analysis = SentimentOperator(task_id="sentiment_analysis")

    feature_engineering = FeatureEngineeringOperator(
        task_id="feature_engineering",
        script_path=GOLD_SCRIPT,
    )

    model_training = ModelTrainingOperator(task_id="model_training")

    start >> [fetch_ohlcv, fetch_news] >> silver_transform
    silver_transform >> [sentiment_analysis, feature_engineering] >> model_training >> end
