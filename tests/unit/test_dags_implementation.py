"""Implementation tests for the Airflow DAG definitions."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module(module_name: str, filename: str):
    module_path = Path(__file__).resolve().parents[2] / "dags" / filename
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeTaskInstance:
    def __init__(self):
        self.calls = []

    def xcom_push(self, key, value):
        self.calls.append((key, value))


def test_btc_market_dag_structure_and_task_callables(monkeypatch) -> None:
    """`btc_market_dag` should wire bronze -> silver -> gold and call the stage helpers."""
    module = _load_module("btc_market_dag_test", "btc_market_dag.py")

    assert module.dag.dag_id == "btc_market_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == set()

    monkeypatch.setattr(module.io_utils, "BASE_DIR", Path("/tmp/btc-dag-test"))
    monkeypatch.setattr(
        module,
        "_get_variable",
        lambda key, default: {
            "BTC_SYMBOL": "BTCUSDT",
            "BTC_INTERVAL": "1d",
            "BTC_START_DATE": "2017-01-01",
        }.get(key, default),
    )

    monkeypatch.setattr(module, "fetch_full_history", lambda *args: [[1, "100"]])
    monkeypatch.setattr(module, "klines_to_dataframe", lambda data: data)
    monkeypatch.setattr(module, "build_gold_features", lambda df: df)

    bronze_calls = []

    def fake_save_data(data, layer, domain="market", symbol="btc_usdt", interval="1d", suffix="", is_json=False):
        bronze_calls.append(
            {
                "layer": layer,
                "domain": domain,
                "symbol": symbol,
                "interval": interval,
                "suffix": suffix,
                "is_json": is_json,
                "data": data,
            }
        )
        output_dir = module.io_utils.BASE_DIR / f"data/{layer}/{domain}"
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / f"{symbol}_{interval}{suffix}.{('json' if is_json else 'csv')}"
        file_path.write_text("[]" if is_json else "open_time,close\n")

    monkeypatch.setattr(module, "save_data", fake_save_data)

    ti = _FakeTaskInstance()

    module.task_bronze(ti=ti)
    module.task_silver(ti=ti)
    module.task_gold(ti=ti)

    assert bronze_calls[0]["layer"] == "bronze"
    assert bronze_calls[0]["is_json"] is True
    assert bronze_calls[1]["layer"] == "silver"
    assert bronze_calls[2]["layer"] == "gold"
    assert ti.calls == [
        ("bronze_output", str(module._bronze_path("1d"))),
        ("silver_output", str(module._silver_path("1d"))),
        ("gold_output", str(module._gold_path("1d"))),
    ]


def test_gdelt_media_dag_structure_and_task_callables(monkeypatch) -> None:
    """`gdelt_media_dag` should wire bronze -> silver -> gold and parse runtime variables."""
    module = _load_module("gdelt_media_dag_test", "gdelt_media_dag.py")

    assert module.dag.dag_id == "gdelt_media_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("bronze").pool == "gdelt_api_pool"

    class FakeBronzeLayer:
        def __init__(self, coin_name, query_terms):
            self.coin_name = coin_name
            self.query_terms = query_terms
            self.output_jsonl = "/tmp/gdelt_bronze.jsonl"
            bronze_calls.append({"coin_name": coin_name, "query_terms": query_terms})

        def run(self, fetch_missing=False):
            bronze_calls[-1]["fetch_missing"] = fetch_missing

    class FakeSilverLayer:
        def __init__(self, coin_name):
            self.coin_name = coin_name
            self.output_csv = "/tmp/gdelt_silver.csv"
            silver_calls.append({"coin_name": coin_name})

        def run(self):
            return None

    class FakeGoldLayer:
        def __init__(self, coin_name, source_mappings=None):
            self.coin_name = coin_name
            self.source_mappings = source_mappings
            self.output_csv = "/tmp/gdelt_gold.csv"
            gold_calls.append({"coin_name": coin_name, "source_mappings": source_mappings})

        def run(self):
            return None

    monkeypatch.setattr(
        module,
        "_get_variable",
        lambda key, default: {
            "GDELT_COIN": "bitcoin",
            "GDELT_QUERY_TERMS": "bitcoin, btc",
            "GDELT_FETCH_MISSING": "true",
            "GDELT_SOURCE_MAPPINGS_JSON": json.dumps({"reuters.com": "Reuters", "cnn.com": "CNN"}),
        }.get(key, default),
    )

    bronze_calls = []
    silver_calls = []
    gold_calls = []

    monkeypatch.setattr(module, "BronzeLayer", FakeBronzeLayer)
    monkeypatch.setattr(module, "SilverLayer", FakeSilverLayer)
    monkeypatch.setattr(module, "GoldLayer", FakeGoldLayer)

    ti = _FakeTaskInstance()

    module.task_bronze(ti=ti)
    module.task_silver(ti=ti)
    module.task_gold(ti=ti)

    assert bronze_calls == [
        {"coin_name": "bitcoin", "query_terms": ["bitcoin", "btc"], "fetch_missing": True}
    ]
    assert silver_calls == [{"coin_name": "bitcoin"}]
    assert gold_calls == [
        {"coin_name": "bitcoin", "source_mappings": {"reuters.com": "Reuters", "cnn.com": "CNN"}}
    ]
    assert ti.calls == [
        ("bronze_output", "/tmp/gdelt_bronze.jsonl"),
        ("silver_output", "/tmp/gdelt_silver.csv"),
        ("gold_output", "/tmp/gdelt_gold.csv"),
    ]
