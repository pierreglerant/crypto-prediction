"""Airflow DAG for GDELT media pipeline (bronze -> silver -> gold)."""

from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

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

from src.ingestion.gdelt.bronze_layer import BronzeLayer  # noqa: E402
from src.ingestion.gdelt.tone_bronze_layer import ToneBronzeLayer  # noqa: E402
from src.processing.media.gold_layer import GoldLayer  # noqa: E402
from src.processing.media.gold_tone_layer import ToneGoldLayer  # noqa: E402
from src.processing.media.gold_tone_merge_layer import GoldToneMergeLayer  # noqa: E402
from src.processing.media.silver_layer import SilverLayer  # noqa: E402
from src.processing.media.silver_tone_layer import ToneSilverLayer  # noqa: E402


def _get_variable(key: str, default: str) -> str:
    """Read an Airflow Variable with Airflow 2/3 compatibility."""
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)


def _parse_query_terms(raw_terms: str) -> list[str]:
    """Parse query terms from a comma-separated Airflow variable."""
    terms = [item.strip() for item in raw_terms.split(",") if item.strip()]
    if not terms:
        raise ValueError("GDELT query terms cannot be empty")
    return terms


def _load_source_mappings() -> dict[str, str] | None:
    """Load optional source mappings from Airflow Variable as JSON."""
    raw = _get_variable("GDELT_SOURCE_MAPPINGS_JSON", "")
    if not raw:
        return None

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("GDELT_SOURCE_MAPPINGS_JSON must be a JSON object")

    return {str(k): str(v) for k, v in parsed.items()}


def _is_tone_enabled() -> bool:
    """Read tone feature toggle from Airflow variables."""
    return _get_variable("GDELT_TONE_ENABLED", "true").strip().lower() == "true"


def task_bronze(**context):
    """Run bronze layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()
    query_terms = _parse_query_terms(_get_variable("GDELT_QUERY_TERMS", "bitcoin,btc"))
    fetch_missing = _get_variable("GDELT_FETCH_MISSING", "false").lower() == "true"

    layer = BronzeLayer(coin_name, query_terms)
    layer.run(fetch_missing=fetch_missing)
    context["ti"].xcom_push(key="bronze_output", value=layer.output_jsonl)


def task_silver(**context):
    """Run silver layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()

    layer = SilverLayer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="silver_output", value=layer.output_csv)


def task_gold(**context):
    """Run gold layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()
    source_mappings = _load_source_mappings()

    layer = GoldLayer(coin_name, source_mappings=source_mappings)
    layer.run()
    context["ti"].xcom_push(key="gold_output", value=layer.output_csv)


def task_tone_bronze(**context):
    """Run tone bronze layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()
    tone_input_csv = _get_variable("GDELT_TONE_INPUT_CSV", "").strip() or None

    layer = ToneBronzeLayer(coin_name, input_csv=tone_input_csv)
    layer.run()
    context["ti"].xcom_push(key="tone_bronze_output", value=layer.output_jsonl)


def task_tone_silver(**context):
    """Run tone silver layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()

    layer = ToneSilverLayer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="tone_silver_output", value=layer.output_csv)


def task_tone_gold(**context):
    """Run tone gold layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()

    layer = ToneGoldLayer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="tone_gold_output", value=layer.output_csv)


def task_merge_gold_tone(**context):
    """Run merge layer and push output path to XCom."""
    coin_name = _get_variable("GDELT_COIN", "bitcoin").strip().lower()

    layer = GoldToneMergeLayer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="gold_tone_output", value=layer.output_csv)


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gdelt_media_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily robust GDELT ingestion and media aggregation pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gdelt", "media", "etl"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=task_bronze,
        pool="gdelt_api_pool",
        execution_timeout=timedelta(minutes=45),
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=task_silver,
        execution_timeout=timedelta(minutes=30),
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=task_gold,
        execution_timeout=timedelta(minutes=30),
    )

    tone_bronze = PythonOperator(
        task_id="tone_bronze",
        python_callable=task_tone_bronze,
        execution_timeout=timedelta(minutes=15),
    )

    tone_silver = PythonOperator(
        task_id="tone_silver",
        python_callable=task_tone_silver,
        execution_timeout=timedelta(minutes=15),
    )

    tone_gold = PythonOperator(
        task_id="tone_gold",
        python_callable=task_tone_gold,
        execution_timeout=timedelta(minutes=15),
    )

    merge_gold_tone = PythonOperator(
        task_id="merge_gold_tone",
        python_callable=task_merge_gold_tone,
        execution_timeout=timedelta(minutes=15),
    )

    if _is_tone_enabled():
        bronze >> silver >> gold >> tone_bronze >> tone_silver >> tone_gold >> merge_gold_tone
    else:
        bronze >> silver >> gold
