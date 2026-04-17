"""Airflow DAG for the combined Binance market-share pipeline."""

from src.ingestion.btc import fetch_full_history
from src.pipelines.airflow_dag_factory import build_market_share_dag
from src.utils.io import save_data

dag = build_market_share_dag(
    fetch_full_history=fetch_full_history,
    save_data=save_data,
)
