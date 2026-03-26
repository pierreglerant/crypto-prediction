"""Unit tests for the ML benchmark pipeline."""

import pandas as pd

from pipelines import ml_benchmark


def make_feature_frame() -> pd.DataFrame:
    """Build a minimal dataframe with features and target for pipeline tests."""
    return pd.DataFrame(
        {
            "feature_a": [1.0, 2.0, 3.0, 4.0],
            "feature_b": [10.0, 20.0, 30.0, 40.0],
            "target": [0, 1, 0, 1],
        }
    )


def test_run_benchmark_pipeline_orchestrates_search_and_benchmark(monkeypatch) -> None:
    """The ML benchmark pipeline should tune then benchmark every registered model."""
    df = make_feature_frame()
    x_train = df[["feature_a", "feature_b"]].iloc[:3]
    y_train = df["target"].iloc[:3]
    split_calls = []
    search_calls = []
    benchmark_calls = []

    expected_models = {
        "dummy": {"builder": object(), "param_space": object()},
        "xgboost": {"builder": object(), "param_space": object()},
    }

    def fake_split(features, target, test_size_ratio):
        split_calls.append((features.copy(), target.copy(), test_size_ratio))
        return features.iloc[:3], features.iloc[3:], target.iloc[:3], target.iloc[3:]

    def fake_search(**kwargs):
        search_calls.append(kwargs)
        return f"trained-{len(search_calls)}"

    def fake_benchmark(model, features, target, tscv, **kwargs):
        benchmark_calls.append(
            {
                "model": model,
                "features": features.copy(),
                "target": target.copy(),
                "tscv": tscv,
                **kwargs,
            }
        )
        return {"pr_auc": 0.5 + len(benchmark_calls) / 10}

    monkeypatch.setattr(ml_benchmark, "train_test_split_time", fake_split)
    monkeypatch.setattr(ml_benchmark, "get_tscv", lambda n_splits: f"tscv-{n_splits}")
    monkeypatch.setattr(ml_benchmark, "get_models", lambda: expected_models)
    monkeypatch.setattr(ml_benchmark, "search_model_optuna", fake_search)
    monkeypatch.setattr(ml_benchmark, "benchmark_model", fake_benchmark)

    results = ml_benchmark.run_benchmark_pipeline(
        df,
        threshold=0.33,
        verbose=False,
        plot_confusion=False,
    )

    assert list(results) == ["dummy", "xgboost"]
    assert results["dummy"] == {"pr_auc": 0.6}
    assert results["xgboost"] == {"pr_auc": 0.7}

    assert len(split_calls) == 1
    split_features, split_target, split_ratio = split_calls[0]
    assert list(split_features.columns) == ["feature_a", "feature_b"]
    assert split_target.tolist() == [0, 1, 0, 1]
    assert split_ratio == ml_benchmark.TEST_SIZE_RATIO

    assert len(search_calls) == 2
    assert [call["model_builder"] for call in search_calls] == [
        expected_models["dummy"]["builder"],
        expected_models["xgboost"]["builder"],
    ]
    assert [call["param_space_fn"] for call in search_calls] == [
        expected_models["dummy"]["param_space"],
        expected_models["xgboost"]["param_space"],
    ]
    assert all(call["X"].equals(x_train) for call in search_calls)
    assert all(call["y"].equals(y_train) for call in search_calls)
    assert all(call["tscv"] == f"tscv-{ml_benchmark.N_SPLITS}" for call in search_calls)
    assert all(call["n_trials"] == ml_benchmark.N_TRIALS for call in search_calls)
    assert all(call["verbose"] is False for call in search_calls)

    assert len(benchmark_calls) == 2
    assert [call["model"] for call in benchmark_calls] == ["trained-1", "trained-2"]
    assert all(call["features"].equals(x_train) for call in benchmark_calls)
    assert all(call["target"].equals(y_train) for call in benchmark_calls)
    assert all(call["tscv"] == f"tscv-{ml_benchmark.N_SPLITS}" for call in benchmark_calls)
    assert all(call["threshold"] == 0.33 for call in benchmark_calls)
    assert all(call["verbose"] is False for call in benchmark_calls)
    assert all(call["plot_confusion"] is False for call in benchmark_calls)


def test_run_benchmark_pipeline_returns_empty_results_when_no_models(monkeypatch) -> None:
    """The ML benchmark pipeline should return an empty mapping when no model is registered."""
    df = make_feature_frame()

    monkeypatch.setattr(
        ml_benchmark,
        "train_test_split_time",
        lambda features, target, _: (features, features.iloc[0:0], target, target.iloc[0:0]),
    )
    monkeypatch.setattr(ml_benchmark, "get_tscv", lambda n_splits: f"tscv-{n_splits}")
    monkeypatch.setattr(ml_benchmark, "get_models", lambda: {})

    results = ml_benchmark.run_benchmark_pipeline(df)

    assert results == {}
