"""Build a daily market-share table across the configured Binance coins."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import pandas as pd

from config.coins import MarketCoinConfig
from processing.btc.silver import klines_to_dataframe


def _to_daily_amount_frame(raw_klines: Sequence[list], coin: MarketCoinConfig) -> pd.DataFrame:
    """Convert raw klines for one coin into a daily quote-volume frame."""
    if not raw_klines:
        return pd.DataFrame(columns=["date", "coin_name", "market_symbol", "artifact_symbol", "amount"])

    df = klines_to_dataframe(list(raw_klines))
    if df.empty:
        return pd.DataFrame(columns=["date", "coin_name", "market_symbol", "artifact_symbol", "amount"])

    daily = (
        df.assign(date=df["open_time"].dt.floor("D"))
        .groupby("date", as_index=False)["quote_asset_volume"]
        .sum()
        .rename(columns={"quote_asset_volume": "amount"})
    )
    daily["coin_name"] = coin.coin_name
    daily["market_symbol"] = coin.market_symbol
    daily["artifact_symbol"] = coin.artifact_symbol

    return daily[["date", "coin_name", "market_symbol", "artifact_symbol", "amount"]]


def build_market_share_table(
    raw_history_by_coin: Mapping[str, Sequence[list]],
    coin_configs: Sequence[MarketCoinConfig],
    *,
    start_date: str = "2015-01-01",
    end_date: str | None = None,
) -> pd.DataFrame:
    """Return a long-form table with per-coin market share by day."""
    start = pd.Timestamp(start_date).normalize()
    if end_date is None:
        end = pd.Timestamp.utcnow().date()
    else:
        end = pd.Timestamp(end_date)

    end = pd.Timestamp(end).normalize()
    date_index = pd.date_range(start=start, end=end, freq="D")

    daily_frames: list[pd.DataFrame] = []
    for coin in coin_configs:
        raw_klines = (
            raw_history_by_coin.get(coin.market_symbol)
            or raw_history_by_coin.get(coin.artifact_symbol)
            or raw_history_by_coin.get(coin.coin_name)
            or []
        )

        daily = _to_daily_amount_frame(raw_klines, coin)
        if daily.empty:
            daily = pd.DataFrame({"date": date_index, "amount": 0.0})
        else:
            daily = daily.set_index("date").reindex(date_index, fill_value=0.0).rename_axis("date").reset_index()

        daily["coin_name"] = coin.coin_name
        daily["market_symbol"] = coin.market_symbol
        daily["artifact_symbol"] = coin.artifact_symbol
        daily_frames.append(daily[["date", "coin_name", "market_symbol", "artifact_symbol", "amount"]])

    combined = pd.concat(daily_frames, ignore_index=True)
    totals = combined.groupby("date", as_index=False)["amount"].sum().rename(columns={"amount": "total_amount"})
    combined = combined.merge(totals, on="date", how="left")
    combined["market_share"] = combined["amount"] / combined["total_amount"].where(combined["total_amount"] != 0)
    combined["market_share"] = combined["market_share"].fillna(0.0)

    return combined.sort_values(["date", "market_symbol"]).reset_index(drop=True)
