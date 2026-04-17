"""Airflow DAG for Litecoin market data pipeline (bronze -> silver -> gold)."""

from config.coins import LTC_MARKET
from ingestion.btc import fetch_full_history
from pipelines.airflow_dag_factory import build_market_dag
from processing.btc.gold import build_gold_features
from processing.btc.silver import klines_to_dataframe
from utils.io import save_data

dag = build_market_dag(
    config=LTC_MARKET,
    fetch_full_history=fetch_full_history,
    klines_to_dataframe=klines_to_dataframe,
    build_gold_features=build_gold_features,
    save_data=save_data,
)
