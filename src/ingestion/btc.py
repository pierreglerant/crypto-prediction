"""Fetch BTC/USDT market data from the Binance API."""

import time
from typing import List

import pandas as pd
import requests

from utils.config import CONFIG

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str, interval: str, start_time: int, limit: int = 1000) -> List[list]:
    """Fetch a batch of klines from Binance."""
    params = {"symbol": symbol, "interval": interval, "startTime": start_time, "limit": limit}

    for _ in range(CONFIG["retry"]):
        try:
            response = requests.get(BINANCE_API_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            time.sleep(1)

    raise Exception("Binance API failed after retries")


def fetch_full_history(symbol: str, interval: str, start_date: str) -> List[list]:
    """Fetch the full kline history starting from a given date."""
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

        time.sleep(CONFIG["sleep"])

    return all_data
