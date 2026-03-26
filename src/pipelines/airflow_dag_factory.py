"""Reusable Airflow DAG factories for coin-based pipelines."""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG

try:
    from airflow.sdk import Variable
except ImportError:  # pragma: no cover
    from airflow.models import Variable

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover
    PythonOperator = importlib.import_module("airflow.operators.python").PythonOperator

from src.pipelines.airflow_common import (  # noqa: E402
    run_gdelt_bronze_task,
    run_gdelt_gold_task,
    run_gdelt_merge_task,
    run_gdelt_silver_task,
    run_gdelt_tone_bronze_task,
    run_gdelt_tone_gold_task,
    run_gdelt_tone_silver_task,
    run_market_bronze_task,
    run_market_gold_task,
    run_market_silver_task,
)


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable with Airflow 2/3 compatibility."""

    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)


def _repo_root() -> Path:
    """Return the repository root inferred from this module location."""

    return Path(__file__).resolve().parents[2]


def _data_root() -> Path:
    """Return the project data root."""

    return _repo_root() / "data"


def build_market_dag(*, config: Any, fetch_full_history, klines_to_dataframe, build_gold_features, save_data) -> DAG:
    """Build a market data DAG for one coin from a config object."""

    def _bronze_path(interval: str) -> Path:
        return _data_root() / "bronze" / "market" / f"{config.artifact_symbol}_{interval}_raw.json"

    def _silver_path(interval: str) -> Path:
        return _data_root() / "silver" / "market" / f"{config.artifact_symbol}_{interval}.csv"

    def _gold_path(interval: str) -> Path:
        return _data_root() / "gold" / "market" / f"{config.artifact_symbol}_{interval}_features.csv"

    def task_bronze(**context):
        """Fetch market klines and persist the bronze artifact."""

        run_market_bronze_task(
            context=context,
            variable_getter=_get_variable,
            coin_symbol=config.variable_prefix,
            market_symbol=config.market_symbol,
            default_interval=config.default_interval,
            default_start_date=config.default_start_date,
            fetch_full_history=fetch_full_history,
            save_data=save_data,
            artifact_symbol=config.artifact_symbol,
            bronze_path=_bronze_path,
        )

    def task_silver(**context):
        """Transform market bronze JSON into silver CSV."""

        run_market_silver_task(
            context=context,
            variable_getter=_get_variable,
            coin_symbol=config.variable_prefix,
            default_interval=config.default_interval,
            klines_to_dataframe=klines_to_dataframe,
            save_data=save_data,
            artifact_symbol=config.artifact_symbol,
            bronze_path=_bronze_path,
            silver_path=_silver_path,
        )

    def task_gold(**context):
        """Build market gold features from the silver dataset."""

        run_market_gold_task(
            context=context,
            variable_getter=_get_variable,
            coin_symbol=config.variable_prefix,
            default_interval=config.default_interval,
            build_gold_features=build_gold_features,
            save_data=save_data,
            artifact_symbol=config.artifact_symbol,
            silver_path=_silver_path,
            gold_path=_gold_path,
        )

    default_args = {
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=config.dag_id,
        default_args=default_args,
        description=config.description,
        start_date=datetime(2024, 1, 1),
        schedule="0 0 * * *",
        catchup=False,
        max_active_runs=1,
        tags=list(config.tags),
    ) as dag:
        bronze = PythonOperator(task_id="bronze", python_callable=task_bronze, execution_timeout=timedelta(minutes=30))
        silver = PythonOperator(task_id="silver", python_callable=task_silver, execution_timeout=timedelta(minutes=15))
        gold = PythonOperator(task_id="gold", python_callable=task_gold, execution_timeout=timedelta(minutes=15))

        bronze >> silver >> gold

    return dag


def build_gdelt_dag(
    *,
    config: Any,
    BronzeLayer,
    SilverLayer,
    GoldLayer,
    ToneBronzeLayer=None,
    ToneSilverLayer=None,
    ToneGoldLayer=None,
    GoldToneMergeLayer=None,
) -> DAG:
    """Build a GDELT media DAG for one coin from a config object."""

    def task_bronze(**context):
        """Run the GDELT bronze layer and push the output path to XCom."""

        run_gdelt_bronze_task(
            context=context,
            variable_getter=_get_variable,
            coin_name=config.coin_name,
            coin_variable=f"{config.variable_prefix}_COIN",
            default_query_terms=config.default_query_terms,
            fetch_missing_var=config.fetch_missing_var,
            bronze_layer=BronzeLayer,
        )

    def task_silver(**context):
        """Run the GDELT silver layer and push the output path to XCom."""

        run_gdelt_silver_task(context=context, coin_name=config.coin_name, silver_layer=SilverLayer)

    def task_gold(**context):
        """Run the GDELT gold layer and push the output path to XCom."""

        run_gdelt_gold_task(
            context=context,
            coin_name=config.coin_name,
            source_mappings_var=config.source_mappings_var,
            variable_getter=_get_variable,
            gold_layer=GoldLayer,
        )

    default_args = {
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=config.dag_id,
        default_args=default_args,
        description=config.description,
        start_date=datetime(2024, 1, 1),
        schedule="0 2 * * *",
        catchup=False,
        max_active_runs=1,
        tags=list(config.tags),
    ) as dag:
        bronze = PythonOperator(task_id="bronze", python_callable=task_bronze, pool="gdelt_api_pool", execution_timeout=timedelta(minutes=45))
        silver = PythonOperator(task_id="silver", python_callable=task_silver, execution_timeout=timedelta(minutes=30))
        gold = PythonOperator(task_id="gold", python_callable=task_gold, execution_timeout=timedelta(minutes=30))

        if config.tone_enabled:
            if ToneBronzeLayer is None or ToneSilverLayer is None or ToneGoldLayer is None or GoldToneMergeLayer is None:
                raise ValueError("Tone layers must be provided when tone_enabled is True")

            def task_tone_bronze(**context):
                """Run the tone bronze layer and push the output path to XCom."""

                run_gdelt_tone_bronze_task(
                    context=context,
                    coin_name=config.coin_name,
                    variable_getter=_get_variable,
                    tone_bronze_layer=ToneBronzeLayer,
                )

            def task_tone_silver(**context):
                """Run the tone silver layer and push the output path to XCom."""

                run_gdelt_tone_silver_task(context=context, coin_name=config.coin_name, tone_silver_layer=ToneSilverLayer)

            def task_tone_gold(**context):
                """Run the tone gold layer and push the output path to XCom."""

                run_gdelt_tone_gold_task(context=context, coin_name=config.coin_name, tone_gold_layer=ToneGoldLayer)

            def task_merge_gold_tone(**context):
                """Merge media gold with tone gold and push the output path to XCom."""

                run_gdelt_merge_task(context=context, coin_name=config.coin_name, merge_layer=GoldToneMergeLayer)

            tone_bronze = PythonOperator(task_id="tone_bronze", python_callable=task_tone_bronze, execution_timeout=timedelta(minutes=15))
            tone_silver = PythonOperator(task_id="tone_silver", python_callable=task_tone_silver, execution_timeout=timedelta(minutes=15))
            tone_gold = PythonOperator(task_id="tone_gold", python_callable=task_tone_gold, execution_timeout=timedelta(minutes=15))
            merge_gold_tone = PythonOperator(task_id="merge_gold_tone", python_callable=task_merge_gold_tone, execution_timeout=timedelta(minutes=15))

            bronze >> silver >> gold >> tone_bronze >> tone_silver >> tone_gold >> merge_gold_tone
        else:
            bronze >> silver >> gold

    return dag
