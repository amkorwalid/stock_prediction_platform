"""
stock_pipeline plugin
~~~~~~~~~~~~~~~~~~~~~
Registers all custom operators with Apache Airflow so they are available
as ``from airflow.operators.stock_pipeline import <Operator>`` and visible
in the Airflow UI operator list.

Operators
---------
FetchOHLCVOperator          Bronze — OHLCV ingestion from Yahoo Finance
FetchNewsOperator           Bronze — News ingestion from Alpha Vantage
SilverTransformOperator     Silver — Bronze → star schema transform
SentimentOperator           Gold   — LLM news sentiment scoring
FeatureEngineeringOperator  Gold   — Technical + price dynamic features
ModelTrainingOperator       Platinum — ML training, registration, predictions

All operators inherit StockBaseOperator which handles:
  - metadata.pipeline_runs  lifecycle (running → success | failed)
  - metadata.etl_pipelines  status + last-run fields
  - audit.system_logs       WARNING+ log lines written to DB
"""

from airflow.plugins_manager import AirflowPlugin

from plugins.base_operator import StockBaseOperator
from plugins.feature_engineering_operator import FeatureEngineeringOperator
from plugins.fetch_news_operator import FetchNewsOperator
from plugins.fetch_ohlcv_operator import FetchOHLCVOperator
from plugins.model_training_operator import ModelTrainingOperator
from plugins.sentiment_operator import SentimentOperator
from plugins.silver_transform_operator import SilverTransformOperator

__all__ = [
    "StockBaseOperator",
    "FetchOHLCVOperator",
    "FetchNewsOperator",
    "SilverTransformOperator",
    "SentimentOperator",
    "FeatureEngineeringOperator",
    "ModelTrainingOperator",
]


class StockPipelinePlugin(AirflowPlugin):
    name = "stock_pipeline"
    operators = [
        FetchOHLCVOperator,
        FetchNewsOperator,
        SilverTransformOperator,
        SentimentOperator,
        FeatureEngineeringOperator,
        ModelTrainingOperator,
    ]
