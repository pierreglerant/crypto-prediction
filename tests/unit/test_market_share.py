"""Unit tests for the combined Binance market-share processor."""

from __future__ import annotations

import pandas as pd

from config.coins import BTC_MARKET, ETH_MARKET
from processing.market_share import build_market_share_table


def _make_raw_kline_row(open_time: int, quote_asset_volume: float) -> list:
    return [
        open_time,
        "100.00",
        "105.00",
        "97.00",
        "102.00",
        "10.00",
        open_time + 86_399_999,
        f"{quote_asset_volume:.2f}",
        100,
        "4.00",
        "408.00",
        "0",
    ]


def test_build_market_share_table_computes_daily_totals_and_shares() -> None:
    """The market-share table should sum daily amounts and normalize shares to 1.0."""
    day_1 = int(pd.Timestamp("2024-01-01").timestamp() * 1000)
    day_2 = int(pd.Timestamp("2024-01-02").timestamp() * 1000)

    raw_history = {
        BTC_MARKET.market_symbol: [
            _make_raw_kline_row(day_1, 100.0),
            _make_raw_kline_row(day_2, 300.0),
        ],
        ETH_MARKET.market_symbol: [
            _make_raw_kline_row(day_1, 300.0),
            _make_raw_kline_row(day_2, 100.0),
        ],
    }

    df = build_market_share_table(raw_history, [BTC_MARKET, ETH_MARKET], start_date="2024-01-01", end_date="2024-01-02")

    assert list(df.columns) == ["date", "coin_name", "market_symbol", "artifact_symbol", "amount", "total_amount", "market_share"]
    assert len(df) == 4

    jan_1 = df[df["date"] == pd.Timestamp("2024-01-01")]
    jan_2 = df[df["date"] == pd.Timestamp("2024-01-02")]

    assert jan_1["total_amount"].nunique() == 1
    assert jan_1["total_amount"].iloc[0] == 400.0
    assert jan_1.loc[jan_1["market_symbol"] == BTC_MARKET.market_symbol, "market_share"].iloc[0] == 0.25
    assert jan_1.loc[jan_1["market_symbol"] == ETH_MARKET.market_symbol, "market_share"].iloc[0] == 0.75

    assert jan_2["total_amount"].nunique() == 1
    assert jan_2["total_amount"].iloc[0] == 400.0
    assert jan_2.loc[jan_2["market_symbol"] == BTC_MARKET.market_symbol, "market_share"].iloc[0] == 0.75
    assert jan_2.loc[jan_2["market_symbol"] == ETH_MARKET.market_symbol, "market_share"].iloc[0] == 0.25

    assert df.groupby("date")["market_share"].sum().round(10).tolist() == [1.0, 1.0]
