#!/usr/bin/env python3
"""Build silver and gold media tone datasets from a daily summary CSV.

The input is expected to contain one row per day and coin with these columns:
`day`, `crypto`, `article_count`, `avg_tone`.

The script cleans the rows, normalizes coin names, drops BCH, and writes
per-coin silver and gold outputs that match the existing repo conventions.

run with "python3 build_media_tone_from_csv.py data/bronze/market_media_all_coins.csv --output-root data" from scripts/
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

SKIPPED_COINS = {"bitcoin_cash", "bch"}
COIN_ALIASES = {
    "ripple": "xrp",
    "xrp": "xrp",
    "bitcoin": "bitcoin",
    "btc": "bitcoin",
    "ethereum": "ethereum",
    "eth": "ethereum",
    "litecoin": "litecoin",
    "ltc": "litecoin",
    "cardano": "cardano",
    "ada": "cardano",
    "dogecoin": "dogecoin",
    "doge": "dogecoin",
}


@dataclass(frozen=True)
class DailyToneRow:
    """Normalized daily media summary for one coin."""

    date: str
    crypto: str
    article_count: int
    avg_tone: float


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Build silver/gold media tone datasets from a daily summary CSV.")
    parser.add_argument("input_csv", help="Path to market_media_all_coins.csv")
    parser.add_argument(
        "--output-root",
        default="data",
        help="Root directory for outputs (default: data)",
    )
    parser.add_argument(
        "--coins",
        nargs="*",
        default=["bitcoin", "ethereum", "xrp", "litecoin", "cardano", "dogecoin"],
        help="Coins to keep in the final outputs (default: all supported coins except BCH)",
    )
    return parser.parse_args()


def repo_root() -> Path:
    """Return the repository root path."""
    return Path(__file__).resolve().parents[1]


def resolve_path(path_str: str, *, base_dir: Path) -> Path:
    """Resolve a path against the cwd, script directory, or repo root."""
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate

    for root in (Path.cwd(), Path(__file__).resolve().parent, base_dir):
        resolved = root / candidate
        if resolved.exists():
            return resolved

    return base_dir / candidate


def resolve_output_path(path_str: str, *, base_dir: Path) -> Path:
    """Resolve an output path under the repository root when relative."""
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate

    return base_dir / candidate


def normalize_crypto(raw_crypto: str) -> str | None:
    """Map source coin labels to the repository canonical names."""
    if not raw_crypto:
        return None

    normalized = raw_crypto.strip().lower()
    if normalized in SKIPPED_COINS:
        return None

    return COIN_ALIASES.get(normalized, normalized)


def normalize_date(raw_date: str) -> str | None:
    """Normalize a date to YYYY-MM-DD."""
    if not raw_date:
        return None

    try:
        return datetime.strptime(raw_date.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def load_rows(input_csv: Path, allowed_coins: set[str]) -> list[DailyToneRow]:
    """Load and clean rows from the source CSV."""
    rows: list[DailyToneRow] = []

    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            date_val = normalize_date(raw_row.get("day", ""))
            crypto_val = normalize_crypto(raw_row.get("crypto", ""))

            if not date_val or not crypto_val or crypto_val not in allowed_coins:
                continue

            try:
                article_count = int(float(raw_row.get("article_count", "")))
                avg_tone = float(raw_row.get("avg_tone", ""))
            except (TypeError, ValueError):
                continue

            rows.append(DailyToneRow(date=date_val, crypto=crypto_val, article_count=article_count, avg_tone=avg_tone))

    return rows


def aggregate_rows(rows: list[DailyToneRow]) -> dict[str, list[DailyToneRow]]:
    """Aggregate duplicate coin/day rows with article-count weighting."""
    grouped: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: {"count": 0.0, "weighted_tone": 0.0}))

    for row in rows:
        cell = grouped[row.crypto][row.date]
        cell["count"] += row.article_count
        cell["weighted_tone"] += row.avg_tone * row.article_count

    normalized: dict[str, list[DailyToneRow]] = {}
    for crypto, by_date in grouped.items():
        crypto_rows: list[DailyToneRow] = []
        for date_val in sorted(by_date.keys()):
            cell = by_date[date_val]
            total_count = int(cell["count"])
            avg_tone = cell["weighted_tone"] / total_count if total_count else 0.0
            crypto_rows.append(DailyToneRow(date=date_val, crypto=crypto, article_count=total_count, avg_tone=avg_tone))

        normalized[crypto] = crypto_rows

    return normalized


def fill_date_gaps(rows: list[DailyToneRow], crypto: str) -> list[DailyToneRow]:
    """Fill missing days in a per-coin series with article_count=0, avg_tone=0.0.

    Days without media coverage are treated as silent days (no articles published,
    neutral tone). This ensures a continuous daily series so that rolling means and
    lags are computed on the correct calendar distances rather than across gaps.
    """
    if not rows:
        return rows

    by_date = {row.date: row for row in rows}
    first = date.fromisoformat(min(by_date))
    last = date.fromisoformat(max(by_date))

    filled: list[DailyToneRow] = []
    current = first
    while current <= last:
        day_str = current.isoformat()
        if day_str in by_date:
            filled.append(by_date[day_str])
        else:
            filled.append(DailyToneRow(date=day_str, crypto=crypto, article_count=0, avg_tone=0.0))
        current += timedelta(days=1)

    return filled


def rolling_mean(values: list[float], idx: int, window: int) -> float | None:
    """Return the trailing rolling mean ending at idx."""
    start_idx = idx - window + 1
    if start_idx < 0:
        return None

    window_values = values[start_idx : idx + 1]
    return sum(window_values) / window


def lag(values: list[float], idx: int, lag_size: int) -> float | None:
    """Return a lagged value from the same sequence."""
    lag_idx = idx - lag_size
    if lag_idx < 0:
        return None

    return values[lag_idx]


def write_silver(output_path: Path, rows: list[DailyToneRow]) -> None:
    """Write the cleaned daily summary rows, with gap days filled as zero-coverage."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    crypto = rows[0].crypto if rows else ""
    filled = fill_date_gaps(rows, crypto)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "avg_tone", "article_count"])
        writer.writeheader()
        for row in filled:
            writer.writerow({"date": row.date, "avg_tone": row.avg_tone, "article_count": row.article_count})


def write_gold(output_path: Path, rows: list[DailyToneRow]) -> None:
    """Write enriched tone features for one coin, with gap days filled as zero-coverage."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    crypto = rows[0].crypto if rows else ""
    ordered_rows = fill_date_gaps(sorted(rows, key=lambda item: item.date), crypto)
    avg_tones = [row.avg_tone for row in ordered_rows]
    article_counts = [row.article_count for row in ordered_rows]

    feature_rows: list[dict[str, object]] = []
    for idx, row in enumerate(ordered_rows):
        feature_row = {
            "date": row.date,
            "avg_tone": row.avg_tone,
            "article_count": row.article_count,
            "tone_ma_7": rolling_mean(avg_tones, idx, 7),
            "tone_ma_30": rolling_mean(avg_tones, idx, 30),
            "tone_lag_1": lag(avg_tones, idx, 1),
            "tone_lag_7": lag(avg_tones, idx, 7),
            "article_count_ma_3": rolling_mean(article_counts, idx, 3),
            "article_count_lag_1": lag(article_counts, idx, 1),
        }

        required_fields = [
            "tone_ma_7",
            "tone_ma_30",
            "tone_lag_1",
            "tone_lag_7",
            "article_count_ma_3",
            "article_count_lag_1",
        ]
        if any(feature_row[field] is None for field in required_fields):
            continue

        feature_rows.append(feature_row)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "avg_tone",
                "article_count",
                "tone_ma_7",
                "tone_ma_30",
                "tone_lag_1",
                "tone_lag_7",
                "article_count_ma_3",
                "article_count_lag_1",
            ],
        )
        writer.writeheader()
        for row in feature_rows:
            writer.writerow(row)


def main() -> None:
    """Run the one-shot conversion."""
    args = parse_args()
    root_dir = repo_root()
    input_csv = resolve_path(args.input_csv, base_dir=root_dir)
    output_root = resolve_output_path(args.output_root, base_dir=root_dir)
    allowed_coins = {coin.strip().lower() for coin in args.coins if coin.strip()}

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    rows = load_rows(input_csv, allowed_coins)
    aggregated = aggregate_rows(rows)

    silver_root = output_root / "silver"
    gold_root = output_root / "gold"

    total_input_rows = len(rows)
    total_output_rows = 0

    print(f"Input: {input_csv}")
    print(f"Output root: {output_root}")
    print(f"Allowed coins: {', '.join(sorted(allowed_coins))}")
    print()

    for coin in sorted(aggregated.keys()):
        coin_rows = aggregated[coin]
        if not coin_rows:
            continue

        silver_path = silver_root / f"{coin}_tone_silver.csv"
        gold_path = gold_root / f"{coin}_tone_gold.csv"

        write_silver(silver_path, coin_rows)
        write_gold(gold_path, coin_rows)

        total_output_rows += len(coin_rows)
        print(f"{coin}: silver={silver_path} gold={gold_path} rows={len(coin_rows)}")

    print()
    print(f"Loaded rows: {total_input_rows}")
    print(f"Written rows: {total_output_rows}")


if __name__ == "__main__":
    main()
