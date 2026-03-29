"""Implementation tests for the Airflow DAG definitions."""

from __future__ import annotations

import importlib.util
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


_GDELT_CALLS: dict[str, list[dict[str, object]]] = {}


def _reset_gdelt_calls() -> dict[str, list[dict[str, object]]]:
    calls = {
        "bronze": [],
        "silver": [],
        "gold": [],
        "tone_bronze": [],
        "tone_silver": [],
        "tone_gold": [],
        "merge": [],
    }
    _GDELT_CALLS.clear()
    _GDELT_CALLS.update(calls)
    return calls


class _FakeBronzeLayer:
    def __init__(self, coin_name, query_terms):
        self.coin_name = coin_name
        self.query_terms = query_terms
        self.output_jsonl = "/tmp/gdelt_bronze.jsonl"
        _GDELT_CALLS["bronze"].append({"coin_name": coin_name, "query_terms": query_terms})

    def run(self, fetch_missing=False):
        _GDELT_CALLS["bronze"][-1]["fetch_missing"] = fetch_missing


class _FakeSilverLayer:
    def __init__(self, coin_name):
        self.coin_name = coin_name
        self.output_csv = "/tmp/gdelt_silver.csv"
        _GDELT_CALLS["silver"].append({"coin_name": coin_name})

    def run(self):
        return None


class _FakeGoldLayer:
    def __init__(self, coin_name, source_mappings=None):
        self.coin_name = coin_name
        self.source_mappings = source_mappings
        self.output_csv = "/tmp/gdelt_gold.csv"
        _GDELT_CALLS["gold"].append({"coin_name": coin_name, "source_mappings": source_mappings})

    def run(self):
        return None


class _FakeToneBronzeLayer:
    def __init__(self, coin_name, input_csv=None, output_jsonl=None):
        self.coin_name = coin_name
        self.input_csv = input_csv
        self.output_jsonl = "/tmp/gdelt_tone_bronze.jsonl"
        _GDELT_CALLS["tone_bronze"].append({"coin_name": coin_name, "input_csv": input_csv})

    def run(self):
        return None


class _FakeToneSilverLayer:
    def __init__(self, coin_name, input_file=None, output_csv=None):
        self.coin_name = coin_name
        self.output_csv = "/tmp/gdelt_tone_silver.csv"
        _GDELT_CALLS["tone_silver"].append({"coin_name": coin_name})

    def run(self):
        return None


class _FakeToneGoldLayer:
    def __init__(self, coin_name, input_csv=None, output_csv=None):
        self.coin_name = coin_name
        self.output_csv = "/tmp/gdelt_tone_gold.csv"
        _GDELT_CALLS["tone_gold"].append({"coin_name": coin_name})

    def run(self):
        return None


class _FakeMergeLayer:
    def __init__(self, coin_name, media_gold_csv=None, tone_gold_csv=None, output_csv=None):
        self.coin_name = coin_name
        self.output_csv = "/tmp/gdelt_gold_with_tone.csv"
        _GDELT_CALLS["merge"].append({"coin_name": coin_name})

    def run(self):
        return None


def test_btc_market_dag_structure_and_task_callables(monkeypatch) -> None:
    """`btc_market_dag` should wire bronze -> silver -> gold and call the stage helpers."""
    module = _load_module("btc_market_dag_test", "btc_market_dag.py")

    assert module.dag.dag_id == "btc_market_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == set()


def test_eth_market_dag_structure_and_task_callables(monkeypatch) -> None:
    """`eth_market_dag` should wire bronze -> silver -> gold and call the stage helpers."""
    module = _load_module("eth_market_dag_test", "eth_market_dag.py")

    assert module.dag.dag_id == "eth_market_pipeline"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == set()


def test_market_share_dag_structure() -> None:
    """`market_share_dag` should fan out across all coins and fan in to one aggregation task."""
    module = _load_module("market_share_dag_test", "market_share_dag.py")

    assert module.dag.dag_id == "market_share_pipeline"
    assert set(module.dag.task_dict) == {
        "fetch_btc_usdt",
        "fetch_eth_usdt",
        "fetch_xrp_usdt",
        "fetch_ltc_usdt",
        "fetch_bch_usdt",
        "fetch_ada_usdt",
        "fetch_doge_usdt",
        "aggregate",
    }
    for task_id in [
        "fetch_btc_usdt",
        "fetch_eth_usdt",
        "fetch_xrp_usdt",
        "fetch_ltc_usdt",
        "fetch_bch_usdt",
        "fetch_ada_usdt",
        "fetch_doge_usdt",
    ]:
        assert module.dag.get_task(task_id).downstream_task_ids == {"aggregate"}
    assert module.dag.get_task("aggregate").downstream_task_ids == set()


def test_gdelt_media_dag_structure() -> None:
    """`gdelt_media_dag` should wire bronze -> silver -> gold and tone tasks."""
    module = _load_module("gdelt_media_dag_test", "gdelt_media_dag.py")

    assert module.dag.dag_id == "gdelt_media_dag"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold", "tone_bronze", "tone_silver", "tone_gold", "merge_gold_tone"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("silver").downstream_task_ids == {"gold"}
    assert module.dag.get_task("gold").downstream_task_ids == {"tone_bronze"}
    assert module.dag.get_task("tone_bronze").downstream_task_ids == {"tone_silver"}
    assert module.dag.get_task("tone_silver").downstream_task_ids == {"tone_gold"}
    assert module.dag.get_task("tone_gold").downstream_task_ids == {"merge_gold_tone"}
    assert module.dag.get_task("merge_gold_tone").downstream_task_ids == set()
    assert module.dag.get_task("bronze").pool == "gdelt_api_pool"


def test_gdelt_media_dag_task_callables_and_runtime_parsing(monkeypatch) -> None:
    """`gdelt_media_dag` should expose the tone-enabled DAG structure."""
    module = _load_module("gdelt_media_dag_test_runtime", "gdelt_media_dag.py")

    assert module.dag.dag_id == "gdelt_media_dag"
    assert set(module.dag.task_dict) == {"bronze", "silver", "gold", "tone_bronze", "tone_silver", "tone_gold", "merge_gold_tone"}
    assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
    assert module.dag.get_task("gold").downstream_task_ids == {"tone_bronze"}


def test_additional_gdelt_dags_structure() -> None:
    """The extra coin DAGs should expose the same tone-enabled task shape."""
    expected = {
        "gdelt_media_eth_dag.py": "gdelt_media_eth_dag",
        "gdelt_media_xrp_dag.py": "gdelt_media_xrp_dag",
        "gdelt_media_ltc_dag.py": "gdelt_media_ltc_dag",
        "gdelt_media_bch_dag.py": "gdelt_media_bch_dag",
        "gdelt_media_ada_dag.py": "gdelt_media_ada_dag",
        "gdelt_media_doge_dag.py": "gdelt_media_doge_dag",
    }

    for filename, dag_id in expected.items():
        module = _load_module(filename.removesuffix(".py"), filename)
        assert module.dag.dag_id == dag_id
        assert set(module.dag.task_dict) == {"bronze", "silver", "gold", "tone_bronze", "tone_silver", "tone_gold", "merge_gold_tone"}
        assert module.dag.get_task("bronze").downstream_task_ids == {"silver"}
        assert module.dag.get_task("gold").downstream_task_ids == {"tone_bronze"}
