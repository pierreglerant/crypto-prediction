"""Unit tests for ETH bronze ingestion."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests

from ingestion import btc as eth_bronze


class _Response:
    def __init__(self, payload: Any):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


def test_fetch_klines_retries_once_then_succeeds(monkeypatch) -> None:
    """`fetch_klines` should retry transient request failures for ETH."""
    calls = []
    responses = [requests.exceptions.RequestException("boom"), _Response([[1, "2"]])]

    monkeypatch.setitem(eth_bronze.CONFIG, "retry", 2)
    monkeypatch.setattr(eth_bronze.time, "sleep", lambda *_args, **_kwargs: None)

    def fake_get(url, params, timeout):
        calls.append((url, params, timeout))
        next_item = responses.pop(0)
        if isinstance(next_item, Exception):
            raise next_item
        return next_item

    monkeypatch.setattr(eth_bronze.requests, "get", fake_get)

    payload = eth_bronze.fetch_klines("ETHUSDT", "1d", 1234567890, limit=50)

    assert payload == [[1, "2"]]
    assert len(calls) == 2
    assert calls[0][1] == {
        "symbol": "ETHUSDT",
        "interval": "1d",
        "startTime": 1234567890,
        "limit": 50,
    }


def test_fetch_full_history_paginates_batches(monkeypatch) -> None:
    """`fetch_full_history` should keep paging until a short batch is returned for ETH."""
    first_page = [[index, "100"] for index in range(1000)]
    second_page = [[1000, "102"]]
    start_times = []

    def fake_fetch_klines(symbol, interval, start_time, limit=1000):
        start_times.append(start_time)
        if len(start_times) == 1:
            return first_page
        if len(start_times) == 2:
            return second_page
        return []

    monkeypatch.setattr(eth_bronze, "fetch_klines", fake_fetch_klines)
    monkeypatch.setattr(eth_bronze.time, "sleep", lambda *_args, **_kwargs: None)

    history = eth_bronze.fetch_full_history("ETHUSDT", "1d", "2024-01-01")

    assert history == first_page + second_page
    assert start_times[0] == int(pd.Timestamp("2024-01-01").timestamp() * 1000)
    assert start_times[1] == first_page[-1][0] + 1
