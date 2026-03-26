"""Shared Airflow task helpers for reusable crypto DAGs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pandas as pd


def get_airflow_variable(variable_getter: Callable[[str, str], str], key: str, default: str) -> str:
    """Read an Airflow Variable with Airflow 2/3 compatibility."""
    try:
        return variable_getter(key, default=default)
    except TypeError:
        return variable_getter(key, default_var=default)


def parse_csv_terms(raw_terms: str) -> list[str]:
    """Parse a comma-separated list of search terms."""
    terms = [item.strip() for item in raw_terms.split(",") if item.strip()]
    if not terms:
        raise ValueError("Query terms cannot be empty")
    return terms


def load_json_mapping(raw_json: str) -> dict[str, str] | None:
    """Parse an optional JSON mapping of source names."""
    if not raw_json:
        return None

    parsed = json.loads(raw_json)
    if not isinstance(parsed, dict):
        raise ValueError("Source mappings must be a JSON object")

    return {str(key): str(value) for key, value in parsed.items()}


def market_data_root(base_dir: Path) -> Path:
    """Return the shared data root for market DAGs."""
    return base_dir / "data"


def gdelt_data_root(base_dir: Path) -> Path:
    """Return the shared data root for GDELT DAGs."""
    return base_dir / "data"


def run_market_bronze_task(
    *,
    context,
    variable_getter,
    coin_symbol: str,
    market_symbol: str,
    default_interval: str,
    default_start_date: str,
    fetch_full_history,
    save_data,
    artifact_symbol: str,
    bronze_path: Callable[[str], Path],
) -> None:
    """Execute the market bronze step and push the output path to XCom."""
    interval = get_airflow_variable(variable_getter, f"{coin_symbol}_INTERVAL", default_interval).strip()
    start_date = get_airflow_variable(variable_getter, f"{coin_symbol}_START_DATE", default_start_date).strip()
    symbol = get_airflow_variable(variable_getter, f"{coin_symbol}_SYMBOL", market_symbol).strip().upper()

    raw_data = fetch_full_history(symbol, interval, start_date)
    save_data(raw_data, "bronze", symbol=artifact_symbol, interval=interval, suffix="_raw", is_json=True)
    context["ti"].xcom_push(key="bronze_output", value=str(bronze_path(interval)))


def run_market_silver_task(
    *,
    context,
    variable_getter,
    coin_symbol: str,
    default_interval: str,
    klines_to_dataframe,
    save_data,
    artifact_symbol: str,
    bronze_path: Callable[[str], Path],
    silver_path: Callable[[str], Path],
) -> None:
    """Execute the market silver step and push the output path to XCom."""
    interval = get_airflow_variable(variable_getter, f"{coin_symbol}_INTERVAL", default_interval).strip()

    with open(bronze_path(interval), encoding="utf-8") as handle:
        raw_data = json.load(handle)

    df_silver = klines_to_dataframe(raw_data)
    save_data(df_silver, "silver", symbol=artifact_symbol, interval=interval)
    context["ti"].xcom_push(key="silver_output", value=str(silver_path(interval)))


def run_market_gold_task(
    *,
    context,
    variable_getter,
    coin_symbol: str,
    default_interval: str,
    build_gold_features,
    save_data,
    artifact_symbol: str,
    silver_path: Callable[[str], Path],
    gold_path: Callable[[str], Path],
) -> None:
    """Execute the market gold step and push the output path to XCom."""
    interval = get_airflow_variable(variable_getter, f"{coin_symbol}_INTERVAL", default_interval).strip()

    df_silver = pd.read_csv(silver_path(interval), parse_dates=["open_time"])
    df_gold = build_gold_features(df_silver)
    save_data(df_gold, "gold", symbol=artifact_symbol, interval=interval, suffix="_features")
    context["ti"].xcom_push(key="gold_output", value=str(gold_path(interval)))


def run_gdelt_bronze_task(
    *,
    context,
    variable_getter,
    coin_name: str,
    coin_variable: str,
    default_query_terms: tuple[str, ...],
    fetch_missing_var: str,
    bronze_layer,
) -> None:
    """Execute the GDELT bronze step and push the output path to XCom."""
    query_terms = parse_csv_terms(
        get_airflow_variable(variable_getter, coin_variable.replace("COIN", "QUERY_TERMS"), ",".join(default_query_terms))
    )
    fetch_missing = get_airflow_variable(variable_getter, fetch_missing_var, "false").lower() == "true"

    layer = bronze_layer(coin_name, query_terms)
    layer.run(fetch_missing=fetch_missing)
    context["ti"].xcom_push(key="bronze_output", value=layer.output_jsonl)


def run_gdelt_silver_task(*, context, coin_name: str, silver_layer) -> None:
    """Execute the GDELT silver step and push the output path to XCom."""
    layer = silver_layer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="silver_output", value=layer.output_csv)


def run_gdelt_gold_task(*, context, coin_name: str, source_mappings_var: str, variable_getter, gold_layer) -> None:
    """Execute the GDELT gold step and push the output path to XCom."""
    raw = get_airflow_variable(variable_getter, source_mappings_var, "")
    source_mappings = load_json_mapping(raw) if raw else None

    layer = gold_layer(coin_name, source_mappings=source_mappings)
    layer.run()
    context["ti"].xcom_push(key="gold_output", value=layer.output_csv)


def run_gdelt_tone_bronze_task(*, context, coin_name: str, variable_getter, tone_bronze_layer) -> None:
    """Execute the tone bronze step and push the output path to XCom."""
    layer = tone_bronze_layer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="tone_bronze_output", value=layer.output_jsonl)


def run_gdelt_tone_silver_task(*, context, coin_name: str, tone_silver_layer) -> None:
    """Execute the tone silver step and push the output path to XCom."""
    layer = tone_silver_layer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="tone_silver_output", value=layer.output_csv)


def run_gdelt_tone_gold_task(*, context, coin_name: str, tone_gold_layer) -> None:
    """Execute the tone gold step and push the output path to XCom."""
    layer = tone_gold_layer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="tone_gold_output", value=layer.output_csv)


def run_gdelt_merge_task(*, context, coin_name: str, merge_layer) -> None:
    """Execute the GDELT merge step and push the output path to XCom."""
    layer = merge_layer(coin_name)
    layer.run()
    context["ti"].xcom_push(key="gold_tone_output", value=layer.output_csv)