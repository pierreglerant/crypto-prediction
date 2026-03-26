"""Airflow DAG for Ethereum GDELT media pipeline (bronze -> silver -> gold)."""

from src.config.coins import ETH_GDELT
from src.ingestion.gdelt.bronze_layer import BronzeLayer
from src.pipelines.airflow_dag_factory import build_gdelt_dag
from src.processing.media.gold_layer import GoldLayer
from src.processing.media.silver_layer import SilverLayer


dag = build_gdelt_dag(
    config=ETH_GDELT,
    BronzeLayer=BronzeLayer,
    SilverLayer=SilverLayer,
    GoldLayer=GoldLayer,
)
