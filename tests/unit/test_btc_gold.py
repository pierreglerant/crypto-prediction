"""Unit tests for BTC gold feature engineering."""

import pandas as pd
from conftest import make_raw_klines

from processing.btc.gold import build_gold_features
from processing.btc.silver import klines_to_dataframe


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
