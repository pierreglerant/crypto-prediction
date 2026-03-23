"""Unit tests for the BTC data engineering pipeline."""

import pandas as pd
from conftest import make_raw_klines

from pipelines import btc as btc_pipeline
from processing.btc.gold import build_gold_features
from processing.btc.silver import klines_to_dataframe


def test_klines_to_dataframe_sorts_and_cleans_rows() -> None:
    """`klines_to_dataframe` should sort rows, coerce dtypes, and drop duplicates."""
    raw_data = make_raw_klines(3)
    duplicated_first = raw_data[0].copy()
    shuffled_data = [raw_data[2], duplicated_first, raw_data[1], raw_data[0]]

    df = klines_to_dataframe(shuffled_data)

    assert list(df["open_time"]) == sorted(df["open_time"].tolist())
    assert len(df) == 3
    assert "close_time" not in df.columns
    assert "ignore" not in df.columns
    assert pd.api.types.is_float_dtype(df["close"])
    assert str(df["number_of_trades"].dtype) == "Int64"


def test_build_gold_features_creates_expected_columns() -> None:
    """`build_gold_features` should derive lagged features and a boolean target."""
    df_silver = klines_to_dataframe(make_raw_klines(50))

    df_gold = build_gold_features(df_silver)

    expected_columns = {
        "return_1d",
        "return_7d",
        "volatility_7d",
        "volatility_30d",
        "drawdown",
        "volume_norm",
        "buy_pressure",
        "lag_return_1d",
        "lag_return_7d",
        "lag_volatility_7d",
        "lag_volume_norm",
        "lag_buy_pressure",
        "target",
    }

    assert expected_columns.issubset(df_gold.columns)
    assert "future_min" not in df_gold.columns
    assert "future_drawdown" not in df_gold.columns
    assert "cummax" not in df_gold.columns
    assert not df_gold.empty
    assert pd.api.types.is_bool_dtype(df_gold["target"])


def test_pipeline_main_calls_stages_in_order(monkeypatch) -> None:
    """`main` should orchestrate ingestion, silver, gold, and persistence in order."""
    raw_data = make_raw_klines(5)
    silver_df = pd.DataFrame({"open_time": pd.to_datetime(["2024-01-01"]), "close": [100.0]})
    gold_df = pd.DataFrame({"target": [True]})
    saved_calls = []

    monkeypatch.setattr(btc_pipeline, "fetch_full_history", lambda *args: raw_data)
    monkeypatch.setattr(btc_pipeline, "klines_to_dataframe", lambda data: silver_df if data is raw_data else None)
    monkeypatch.setattr(btc_pipeline, "build_gold_features", lambda df: gold_df if df is silver_df else None)
    monkeypatch.setattr(
        btc_pipeline,
        "save_data",
        lambda data, layer, **kwargs: saved_calls.append((layer, data, kwargs)),
    )

    btc_pipeline.main()

    assert [call[0] for call in saved_calls] == ["bronze", "silver", "gold"]
    assert saved_calls[0][1] is raw_data
    assert saved_calls[0][2] == {"suffix": "_raw", "is_json": True}
    assert saved_calls[1][1] is silver_df
    assert saved_calls[1][2] == {}
    assert saved_calls[2][1] is gold_df
    assert saved_calls[2][2] == {"suffix": "_features"}
