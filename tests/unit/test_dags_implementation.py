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


def test_eth_market_dag_structure_and_task_callables(monkeypatch) -> None:
    """`eth_market_dag` should wire bronze -> silver -> gold and call the stage helpers."""
    module = _load_module("eth_market_dag_test", "eth_market_dag.py")

    assert module.dag.dag_id == "eth_market_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == set()

    monkeypatch.setattr(module.io_utils, "BASE_DIR", Path("/tmp/eth-dag-test"))
    monkeypatch.setattr(
        module,
        "_get_variable",
        lambda key, default: {
            "ETH_SYMBOL": "ETHUSDT",
            "ETH_INTERVAL": "1d",
            "ETH_START_DATE": "2017-01-01",
        }.get(key, default),
    )

    monkeypatch.setattr(module, "fetch_full_history", lambda *args: [[1, "100"]])
    monkeypatch.setattr(module, "klines_to_dataframe", lambda data: data)
    monkeypatch.setattr(module, "build_gold_features", lambda df: df)

    save_calls = []

    def fake_save_data(data, layer, domain="market", symbol="eth_usdt", interval="1d", suffix="", is_json=False):
        save_calls.append(
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

    assert save_calls[0]["layer"] == "bronze"
    assert save_calls[0]["symbol"] == "eth_usdt"
    assert save_calls[1]["layer"] == "silver"
    assert save_calls[2]["layer"] == "gold"
    assert ti.calls == [
        ("bronze_output", str(module._bronze_path("1d"))),
        ("silver_output", str(module._silver_path("1d"))),
        ("gold_output", str(module._gold_path("1d"))),
    ]


def test_gdelt_media_dag_structure_and_task_callables(monkeypatch) -> None:
    """`gdelt_media_dag` should wire bronze -> silver -> gold and tone tasks, then parse runtime variables."""
    module = _load_module("gdelt_media_dag_test", "gdelt_media_dag.py")

    assert module.dag.dag_id == "gdelt_media_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold", "tone_bronze", "tone_silver", "tone_gold", "merge_gold_tone"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == {"tone_bronze"}
    assert module.dag.get_task("tone_bronze").downstream_task_ids == {"tone_silver"}
    assert module.dag.get_task("tone_silver").downstream_task_ids == {"tone_gold"}
    assert module.dag.get_task("tone_gold").downstream_task_ids == {"merge_gold_tone"}
    assert module.dag.get_task("merge_gold_tone").downstream_task_ids == set()
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

    class FakeToneBronzeLayer:
        def __init__(self, coin_name, input_csv=None, output_jsonl=None):
            self.coin_name = coin_name
            self.input_csv = input_csv
            self.output_jsonl = "/tmp/gdelt_tone_bronze.jsonl"
            tone_bronze_calls.append({"coin_name": coin_name, "input_csv": input_csv})

        def run(self):
            return None

    class FakeToneSilverLayer:
        def __init__(self, coin_name, input_file=None, output_csv=None):
            self.coin_name = coin_name
            self.output_csv = "/tmp/gdelt_tone_silver.csv"
            tone_silver_calls.append({"coin_name": coin_name})

        def run(self):
            return None

    class FakeToneGoldLayer:
        def __init__(self, coin_name, input_csv=None, output_csv=None):
            self.coin_name = coin_name
            self.output_csv = "/tmp/gdelt_tone_gold.csv"
            tone_gold_calls.append({"coin_name": coin_name})

        def run(self):
            return None

    class FakeMergeLayer:
        def __init__(self, coin_name, media_gold_csv=None, tone_gold_csv=None, output_csv=None):
            self.coin_name = coin_name
            self.output_csv = "/tmp/gdelt_gold_with_tone.csv"
            merge_calls.append({"coin_name": coin_name})

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
    tone_bronze_calls = []
    tone_silver_calls = []
    tone_gold_calls = []
    merge_calls = []

    monkeypatch.setattr(module, "BronzeLayer", FakeBronzeLayer)
    monkeypatch.setattr(module, "SilverLayer", FakeSilverLayer)
    monkeypatch.setattr(module, "GoldLayer", FakeGoldLayer)
    monkeypatch.setattr(module, "ToneBronzeLayer", FakeToneBronzeLayer)
    monkeypatch.setattr(module, "ToneSilverLayer", FakeToneSilverLayer)
    monkeypatch.setattr(module, "ToneGoldLayer", FakeToneGoldLayer)
    monkeypatch.setattr(module, "GoldToneMergeLayer", FakeMergeLayer)

    ti = _FakeTaskInstance()

    module.task_bronze(ti=ti)
    module.task_silver(ti=ti)
    module.task_gold(ti=ti)
    module.task_tone_bronze(ti=ti)
    module.task_tone_silver(ti=ti)
    module.task_tone_gold(ti=ti)
    module.task_merge_gold_tone(ti=ti)

    assert bronze_calls == [{"coin_name": "bitcoin", "query_terms": ["bitcoin", "btc"], "fetch_missing": True}]
    assert silver_calls == [{"coin_name": "bitcoin"}]
    assert gold_calls == [{"coin_name": "bitcoin", "source_mappings": {"reuters.com": "Reuters", "cnn.com": "CNN"}}]
    assert tone_bronze_calls == [{"coin_name": "bitcoin", "input_csv": None}]
    assert tone_silver_calls == [{"coin_name": "bitcoin"}]
    assert tone_gold_calls == [{"coin_name": "bitcoin"}]
    assert merge_calls == [{"coin_name": "bitcoin"}]
    assert ti.calls == [
        ("bronze_output", "/tmp/gdelt_bronze.jsonl"),
        ("silver_output", "/tmp/gdelt_silver.csv"),
        ("gold_output", "/tmp/gdelt_gold.csv"),
        ("tone_bronze_output", "/tmp/gdelt_tone_bronze.jsonl"),
        ("tone_silver_output", "/tmp/gdelt_tone_silver.csv"),
        ("tone_gold_output", "/tmp/gdelt_tone_gold.csv"),
        ("gold_tone_output", "/tmp/gdelt_gold_with_tone.csv"),
    ]
