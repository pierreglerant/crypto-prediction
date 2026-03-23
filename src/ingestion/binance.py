"""Fetch historical BTC/USDT klines from the Binance API.

Save bronze (raw), silver (clean), and gold (features) datasets.
"""

import json
import time
from pathlib import Path
from typing import Any, List

import pandas as pd
import requests

# 🔹 Base project directory
BASE_DIR = Path(__file__).resolve().parents[2]

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"


# =========================
# FETCH
# =========================
def fetch_klines(symbol: str, interval: str, start_time: int, limit: int = 1000) -> List[Any]:
    params = {"symbol": symbol, "interval": interval, "startTime": start_time, "limit": limit}
    response = requests.get(BINANCE_API_URL, params=params)
    response.raise_for_status()
    return response.json()


def fetch_full_history(symbol: str, interval: str, start_date: str = "2017-01-01") -> List[Any]:
    start_time = int(pd.Timestamp(start_date).timestamp() * 1000)
    all_data = []

    while True:
        data = fetch_klines(symbol, interval, start_time)

        if not data:
            break

        all_data.extend(data)
        start_time = data[-1][0] + 1

        if len(data) < 1000:
            break

        time.sleep(0.2)

    return all_data


# =========================
# SILVER
# =========================
def klines_to_dataframe(data: List[Any]) -> pd.DataFrame:
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

    df = df.sort_values("open_time").reset_index(drop=True)

    return df


# =========================
# GOLD (FEATURE ENGINEERING)
# =========================
def build_gold_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Returns
    df["return_1d"] = df["close"].pct_change()
    df["return_7d"] = df["close"].pct_change(7)

    # Volatility
    df["volatility_7d"] = df["return_1d"].rolling(7).std()
    df["volatility_30d"] = df["return_1d"].rolling(30).std()

    # Drawdown
    df["cummax"] = df["close"].cummax()
    df["drawdown"] = (df["close"] - df["cummax"]) / df["cummax"]

    # Volume normalization
    df["volume_norm"] = df["volume"] / df["volume"].rolling(30).mean()

    # Buy pressure
    df["buy_pressure"] = df["taker_buy_base_volume"] / df["volume"]

    # Lags
    df["lag_return_1d"] = df["return_1d"].shift(1)
    df["lag_return_7d"] = df["return_7d"].shift(1)
    df["lag_volatility_7d"] = df["volatility_7d"].shift(1)
    df["lag_volume_norm"] = df["volume_norm"].shift(1)
    df["lag_buy_pressure"] = df["buy_pressure"].shift(1)

    # Target
    df["future_min"] = df["close"].rolling(7).min().shift(-7)
    df["future_drawdown"] = (df["future_min"] / df["close"]) - 1
    df["target"] = df["future_drawdown"] < -0.10

    # Clean
    df = df.drop(columns=["cummax"])
    df = df.dropna().reset_index(drop=True)

    return df


# =========================
# SAVE (GENERIC)
# =========================
def save_data(
    data,
    layer: str,
    symbol: str = "btc_usdt",
    interval: str = "1d",
    suffix: str = "",
    is_json: bool = False,
):
    """Generic save function for bronze/silver/gold layers."""
    output_dir = BASE_DIR / f"data/{layer}/market"
    output_dir.mkdir(parents=True, exist_ok=True)

    extension = "json" if is_json else "csv"
    file_path = output_dir / f"{symbol}_{interval}{suffix}.{extension}"

    if is_json:
        with open(file_path, "w") as f:
            json.dump(data, f)
    else:
        data.to_csv(file_path, index=False)

    print(f"{layer.capitalize()} saved to {file_path}")


# =========================
# MAIN
# =========================
def main():
    """Fetch, process, and save Binance BTC/USDT historical data."""
    raw_data = fetch_full_history("BTCUSDT", "1d", "2017-01-01")

    # Bronze
    save_data(raw_data, layer="bronze", suffix="_raw", is_json=True)

    # Silver
    df_silver = klines_to_dataframe(raw_data)
    save_data(df_silver, layer="silver")

    # Gold
    df_gold = build_gold_features(df_silver)
    save_data(df_gold, layer="gold", suffix="_features")

    print(f"Total rows fetched: {len(raw_data)}")
    print(f"Gold dataset shape: {df_gold.shape}")


if __name__ == "__main__":
    main()
