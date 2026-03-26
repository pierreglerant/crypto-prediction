"""Airflow DAG for BTC market data pipeline (bronze -> silver -> gold)."""

from src.config.coins import BTC_MARKET
from src.ingestion.btc import fetch_full_history
from src.pipelines.airflow_dag_factory import build_market_dag
from src.processing.btc.gold import build_gold_features
from src.processing.btc.silver import klines_to_dataframe
from src.utils.io import save_data


dag = build_market_dag(
    config=BTC_MARKET,
    fetch_full_history=fetch_full_history,
    klines_to_dataframe=klines_to_dataframe,
    build_gold_features=build_gold_features,
    save_data=save_data,
)
