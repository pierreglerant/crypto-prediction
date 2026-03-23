"""Fetch historical BTC/USDT klines from the Binance API.

Save both bronze (raw) and silver (clean) datasets.
"""

import json
import time
from pathlib import Path
from typing import Any, List

import pandas as pd
import requests

# 🔹 Base project directory (robust path handling)
BASE_DIR = Path(__file__).resolve().parents[2]

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str, interval: str, start_time: int, limit: int = 1000) -> List[Any]:
    """Fetch klines data from Binance API."""
    params = {"symbol": symbol, "interval": interval, "startTime": start_time, "limit": limit}

    response = requests.get(BINANCE_API_URL, params=params)
    response.raise_for_status()
    return response.json()


def fetch_full_history(symbol: str, interval: str, start_date: str = "2017-01-01") -> List[Any]:
    """Fetch full historical klines data starting from a specific date."""
    start_time = int(pd.Timestamp(start_date).timestamp() * 1000)
    all_data = []

    while True:
        data = fetch_klines(symbol, interval, start_time)

        if not data:
            break

        all_data.extend(data)
        start_time = data[-1][0] + 1  # move forward

        # stop if last batch
        if len(data) < 1000:
            break

        time.sleep(0.2)  # avoid rate limit

    return all_data


def klines_to_dataframe(data: List[Any]) -> pd.DataFrame:
    """Convert raw klines to clean DataFrame."""
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

    # Convert timestamp
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")

    # Convert numeric columns
    float_cols = ["open", "high", "low", "close", "volume", "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"]

    df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
    df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], errors="coerce").astype("Int64")

    # Drop unnecessary columns
    df.drop(columns=["close_time", "ignore"], inplace=True)

    return df


def save_bronze(data: List[Any], symbol: str = "btc_usdt", interval: str = "1d"):
    """Save raw API data (bronze layer)."""
    output_dir = BASE_DIR / "data/bronze/market/"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{symbol}_{interval}_raw.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Bronze saved to {file_path}")


def save_silver(df: pd.DataFrame, symbol: str = "btc_usdt", interval: str = "1d"):
    """Save cleaned data (silver layer)."""
    output_dir = BASE_DIR / "data/silver/market/"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{symbol}_{interval}.csv"

    df.to_csv(file_path, index=False)

    print(f"Silver saved to {file_path}")


def main():
    """Fetch, transform, and persist BTC/USDT market data."""
    raw_data = fetch_full_history("BTCUSDT", "1d", "2017-01-01")

    # Bronze layer
    save_bronze(raw_data, "btc_usdt", "1d")

    # Silver layer
    df = klines_to_dataframe(raw_data)
    save_silver(df, "btc_usdt", "1d")

    print(f"Total rows fetched: {len(raw_data)}")


if __name__ == "__main__":
    main()
