"""Unit tests for ETH silver processing."""

import pandas as pd
from conftest import make_raw_klines

from processing.btc.silver import klines_to_dataframe


def test_klines_to_dataframe_sorts_and_cleans_rows() -> None:
    """`klines_to_dataframe` should sort rows, coerce dtypes, and drop duplicates for ETH data too."""
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
