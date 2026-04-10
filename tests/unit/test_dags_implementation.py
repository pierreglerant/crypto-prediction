"""Implementation tests for the Airflow DAG definitions."""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path


class _StubDAG:
    """Minimal DAG context manager for loading DAG files without apache-airflow."""

    _current: _StubDAG | None = None

    def __init__(self, dag_id: str, **kwargs: object) -> None:
        self.dag_id = dag_id
        self.task_dict: dict[str, _StubPythonOperator] = {}

    def __enter__(self) -> _StubDAG:
        _StubDAG._current = self
        return self

    def __exit__(self, *args: object) -> bool:
        _StubDAG._current = None
        return False

    def get_task(self, task_id: str) -> _StubPythonOperator:
        return self.task_dict[task_id]


class _StubPythonOperator:
    def __init__(
        self,
        task_id: str,
        python_callable: object,
        execution_timeout: object = None,
        pool: str | None = None,
        **kwargs: object,
    ) -> None:
        dag = _StubDAG._current
        if dag is None:
            raise RuntimeError("PythonOperator used outside DAG context")
        self.task_id = task_id
        self.python_callable = python_callable
        self.downstream_task_ids: set[str] = set()
        self.pool = pool
        dag.task_dict[task_id] = self

    def __rshift__(self, other: object) -> object:
        if isinstance(other, list):
            cur = self
            for t in other:
                cur = cur >> t
            return cur
        self.downstream_task_ids.add(other.task_id)  # type: ignore[attr-defined]
        return other


class _StubVariable:
    @staticmethod
    def get(key: str, default: str | None = None, default_var: str | None = None) -> str:
        val = default if default is not None else default_var
        return val if val is not None else ""


def _register_airflow_stub_modules() -> None:
    """Populate sys.modules with minimal Airflow shims (DAG, PythonOperator, Variable)."""
    airflow_mod = types.ModuleType("airflow")
    airflow_mod.DAG = _StubDAG
    sys.modules["airflow"] = airflow_mod

    sdk_mod = types.ModuleType("airflow.sdk")
    sdk_mod.Variable = _StubVariable
    sys.modules["airflow.sdk"] = sdk_mod

    pyops = types.ModuleType("airflow.providers.standard.operators.python")
    pyops.PythonOperator = _StubPythonOperator
    sys.modules["airflow.providers"] = types.ModuleType("airflow.providers")
    sys.modules["airflow.providers.standard"] = types.ModuleType("airflow.providers.standard")
    sys.modules["airflow.providers.standard.operators"] = types.ModuleType("airflow.providers.standard.operators")
    sys.modules["airflow.providers.standard.operators.python"] = pyops


try:
    import airflow  # noqa: F401
except ImportError:
    _register_airflow_stub_modules()


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
        return None


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
        self.output_csv = "/tmp/gdelt_gold.csv"
        _GDELT_CALLS["gold"].append({"coin_name": coin_name, "source_mappings": source_mappings})

    def run(self):
        return None


class _FakeToneBronzeLayer:
    def __init__(self, coin_name, input_csv=None):
        self.coin_name = coin_name
        self.output_jsonl = "/tmp/gdelt_tone_bronze.jsonl"
        _GDELT_CALLS["tone_bronze"].append({"coin_name": coin_name, "input_csv": input_csv})

    def run(self):
        return None


class _FakeToneSilverLayer:
    def __init__(self, coin_name):
        self.coin_name = coin_name
        self.output_csv = "/tmp/gdelt_tone_silver.csv"
        _GDELT_CALLS["tone_silver"].append({"coin_name": coin_name})

    def run(self):
        return None


class _FakeToneGoldLayer:
    def __init__(self, coin_name):
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


def test_gdelt_media_dag_structure() -> None:
    """`gdelt_media_dag` should wire bronze -> silver -> gold and tone tasks."""
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


def test_gdelt_media_dag_task_callables_and_runtime_parsing(monkeypatch) -> None:
    """`gdelt_media_dag` task callables should parse runtime variables and execute stage layers."""
    module = _load_module("gdelt_media_dag_test_runtime", "gdelt_media_dag.py")

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

    calls = _reset_gdelt_calls()

    monkeypatch.setattr(module, "BronzeLayer", _FakeBronzeLayer)
    monkeypatch.setattr(module, "SilverLayer", _FakeSilverLayer)
    monkeypatch.setattr(module, "GoldLayer", _FakeGoldLayer)
    monkeypatch.setattr(module, "ToneBronzeLayer", _FakeToneBronzeLayer)
    monkeypatch.setattr(module, "ToneSilverLayer", _FakeToneSilverLayer)
    monkeypatch.setattr(module, "ToneGoldLayer", _FakeToneGoldLayer)
    monkeypatch.setattr(module, "GoldToneMergeLayer", _FakeMergeLayer)

    ti = _FakeTaskInstance()

    module.task_bronze(ti=ti)
    module.task_silver(ti=ti)
    module.task_gold(ti=ti)
    module.task_tone_bronze(ti=ti)
    module.task_tone_silver(ti=ti)
    module.task_tone_gold(ti=ti)
    module.task_merge_gold_tone(ti=ti)

    assert calls["bronze"] == [{"coin_name": "bitcoin", "query_terms": ["bitcoin", "btc"], "fetch_missing": True}]
    assert calls["silver"] == [{"coin_name": "bitcoin"}]
    assert calls["gold"] == [{"coin_name": "bitcoin", "source_mappings": {"reuters.com": "Reuters", "cnn.com": "CNN"}}]
    assert calls["tone_bronze"] == [{"coin_name": "bitcoin", "input_csv": None}]
    assert calls["tone_silver"] == [{"coin_name": "bitcoin"}]
    assert calls["tone_gold"] == [{"coin_name": "bitcoin"}]
    assert calls["merge"] == [{"coin_name": "bitcoin"}]
    assert ti.calls == [
        ("bronze_output", "/tmp/gdelt_bronze.jsonl"),
        ("silver_output", "/tmp/gdelt_silver.csv"),
        ("gold_output", "/tmp/gdelt_gold.csv"),
        ("tone_bronze_output", "/tmp/gdelt_tone_bronze.jsonl"),
        ("tone_silver_output", "/tmp/gdelt_tone_silver.csv"),
        ("tone_gold_output", "/tmp/gdelt_tone_gold.csv"),
        ("gold_tone_output", "/tmp/gdelt_gold_with_tone.csv"),
    ]
