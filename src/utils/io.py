"""Utility helpers for persisting pipeline outputs."""

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]


def save_data(
    data,
    layer: str,
    domain: str = "market",
    symbol: str = "btc_usdt",
    interval: str = "1d",
    suffix: str = "",
    is_json: bool = False,
):
    """Save raw or tabular data to the requested pipeline layer."""
    output_dir = BASE_DIR / f"data/{layer}/{domain}"
    output_dir.mkdir(parents=True, exist_ok=True)

    extension = "json" if is_json else "csv"
    file_path = output_dir / f"{symbol}_{interval}{suffix}.{extension}"

    if is_json:
        with open(file_path, "w") as f:
            json.dump(data, f)
    else:
        data.to_csv(file_path, index=False)

    print(f"{layer.capitalize()} saved to {file_path}")
