"""Convert raw BTC klines into a cleaned silver dataset."""

from typing import List

import pandas as pd


def klines_to_dataframe(data: List[list]) -> pd.DataFrame:
    """Transform raw kline rows into a typed DataFrame."""
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]

    df = pd.DataFrame(data, columns=columns)

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")

    float_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
    df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce").astype("Int64")

    df.drop(columns=["close_time", "ignore"], inplace=True)

    df = df.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)

    return df
