"""Airflow DAG for BTC market data pipeline (bronze -> silver -> gold)."""

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG

try:
    # Airflow 3+ location
    from airflow.sdk import Variable
except ImportError:  # pragma: no cover
    # Airflow 2.x compatibility
    from airflow.models import Variable

try:
    # Airflow 3+ location
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    # Airflow 2.x compatibility
    PythonOperator = importlib.import_module("airflow.operators.python").PythonOperator

# Ensure src package imports work when DAG is parsed from Airflow.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.ingestion.btc import fetch_full_history  # noqa: E402
from src.processing.btc.gold import build_gold_features  # noqa: E402
from src.processing.btc.silver import klines_to_dataframe  # noqa: E402
from src.utils import io as io_utils  # noqa: E402
from src.utils.io import save_data  # noqa: E402


def _data_root() -> Path:
    return io_utils.BASE_DIR / "data"


def _bronze_path(interval: str) -> Path:
    return _data_root() / "bronze" / "market" / f"btc_usdt_{interval}_raw.json"


def _silver_path(interval: str) -> Path:
    return _data_root() / "silver" / "market" / f"btc_usdt_{interval}.csv"


def _gold_path(interval: str) -> Path:
    return _data_root() / "gold" / "market" / f"btc_usdt_{interval}_features.csv"


def _artifact_symbol() -> str:
    return "btc_usdt"


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable with Airflow 2/3 compatibility."""
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)


def task_bronze(**context):
    """Fetch klines from Binance, save as JSON, push artifact path to XCom."""
    symbol = _get_variable("BTC_SYMBOL", "BTCUSDT").strip().upper()
    interval = _get_variable("BTC_INTERVAL", "1d").strip()
    start_date = _get_variable("BTC_START_DATE", "2017-01-01").strip()

    raw_data = fetch_full_history(symbol, interval, start_date)
    save_data(raw_data, "bronze", symbol=_artifact_symbol(), interval=interval, suffix="_raw", is_json=True)
    context["ti"].xcom_push(key="bronze_output", value=str(_bronze_path(interval)))


def task_silver(**context):
    """Clean bronze JSON into silver CSV, push artifact path to XCom."""
    interval = _get_variable("BTC_INTERVAL", "1d").strip()

    bronze_path = _bronze_path(interval)
    with open(bronze_path, encoding="utf-8") as handle:
        raw_data = json.load(handle)

    df_silver = klines_to_dataframe(raw_data)
    save_data(df_silver, "silver", symbol=_artifact_symbol(), interval=interval)
    context["ti"].xcom_push(key="silver_output", value=str(_silver_path(interval)))


def task_gold(**context):
    """Engineer features from silver CSV into gold CSV, push artifact path to XCom."""
    interval = _get_variable("BTC_INTERVAL", "1d").strip()

    silver_path = _silver_path(interval)
    df_silver = pd.read_csv(silver_path, parse_dates=["open_time"])
    df_gold = build_gold_features(df_silver)
    save_data(df_gold, "gold", symbol=_artifact_symbol(), interval=interval, suffix="_features")
    context["ti"].xcom_push(key="gold_output", value=str(_gold_path(interval)))


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="btc_market_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily BTC/USDT market data pipeline: Binance ingestion to gold features",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",  # daily at midnight UTC (different from GDELT at 02:00)
    catchup=False,
    max_active_runs=1,
    tags=["btc", "market", "binance", "etl"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=task_bronze,
        execution_timeout=timedelta(minutes=30),
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=task_silver,
        execution_timeout=timedelta(minutes=15),
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=task_gold,
        execution_timeout=timedelta(minutes=15),
    )

    bronze >> silver >> gold
