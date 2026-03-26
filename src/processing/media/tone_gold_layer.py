#!/usr/bin/env python3
"""
GOLD LAYER: Build enriched tone features from daily tone silver data.

Output includes rolling and lag features for ML usage.
"""

import csv
import os
from pathlib import Path


class ToneGoldLayer:
    """Generate daily enriched tone features."""

    def __init__(self, coin_name, input_csv=None, output_csv=None):
        """
        Args.

        coin_name: Name of cryptocurrency
        input_csv: Path to input CSV (default: data/silver/{coin_name}_tone_silver.csv)
        output_csv: Path to output CSV (default: data/gold/{coin_name}_tone_gold.csv)
        """
        self.coin_name = coin_name
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"
        gold_dir = data_root / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)

        self.input_csv = str(input_csv) if input_csv else str(data_root / "silver" / f"{coin_name}_tone_silver.csv")
        self.output_csv = str(output_csv) if output_csv else str(gold_dir / f"{coin_name}_tone_gold.csv")

        self.rows_loaded = 0
        self.rows_written = 0

    def _read_rows(self):
        """Read and type-cast silver rows."""
        rows = []
        with open(self.input_csv, "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                date_val = row.get("date")
                avg_tone_raw = row.get("avg_tone")
                article_count_raw = row.get("article_count")

                if not date_val or avg_tone_raw is None or article_count_raw is None:
                    continue

                try:
                    avg_tone = float(avg_tone_raw)
                    article_count = int(float(article_count_raw))
                except (TypeError, ValueError):
                    continue

                rows.append(
                    {
                        "date": date_val,
                        "avg_tone": avg_tone,
                        "article_count": article_count,
                    }
                )

        rows.sort(key=lambda item: item["date"])
        self.rows_loaded = len(rows)
        return rows

    def _rolling_mean(self, rows, idx, key, window):
        """Compute rolling mean including current index."""
        if idx < window - 1:
            return None

        values = [rows[pos][key] for pos in range(idx - window + 1, idx + 1)]
        return sum(values) / window

    def _lag(self, rows, idx, key, lag_size):
        """Get lagged value from prior row."""
        lag_idx = idx - lag_size
        if lag_idx < 0:
            return None
        return rows[lag_idx][key]

    def _build_feature_rows(self, rows):
        """Create enriched feature rows and drop incomplete lead-in rows."""
        feature_rows = []

        for idx, row in enumerate(rows):
            out = {
                "date": row["date"],
                "avg_tone": row["avg_tone"],
                "article_count": row["article_count"],
                "tone_ma_3": self._rolling_mean(rows, idx, "avg_tone", 3),
                "tone_ma_7": self._rolling_mean(rows, idx, "avg_tone", 7),
                "tone_lag_1": self._lag(rows, idx, "avg_tone", 1),
                "tone_lag_3": self._lag(rows, idx, "avg_tone", 3),
                "article_count_ma_3": self._rolling_mean(rows, idx, "article_count", 3),
                "article_count_lag_1": self._lag(rows, idx, "article_count", 1),
            }

            required = [
                "tone_ma_3",
                "tone_ma_7",
                "tone_lag_1",
                "tone_lag_3",
                "article_count_ma_3",
                "article_count_lag_1",
            ]
            if any(out[key] is None for key in required):
                continue

            feature_rows.append(out)

        return feature_rows

    def run(self):
        """Execute tone gold feature engineering."""
        print(f" === TONE GOLD LAYER: Enrich {self.coin_name.upper()} ===")
        print(f" Input: {self.input_csv}")
        print(f" Output: {self.output_csv}")
        print()

        if not os.path.exists(self.input_csv):
            print(f"❌ File {self.input_csv} not found!")
            return

        try:
            rows = self._read_rows()
            feature_rows = self._build_feature_rows(rows)

            fieldnames = [
                "date",
                "avg_tone",
                "article_count",
                "tone_ma_3",
                "tone_ma_7",
                "tone_lag_1",
                "tone_lag_3",
                "article_count_ma_3",
                "article_count_lag_1",
            ]

            with open(self.output_csv, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in feature_rows:
                    writer.writerow(row)

            self.rows_written = len(feature_rows)

        except Exception as e:
            print(f"❌ Error: {e}")
            return

        print()
        print(" === TONE GOLD SUMMARY ===")
        print(f"Rows loaded: {self.rows_loaded:,}")
        print(f"Rows written: {self.rows_written:,}")
        print(f"Output: {self.output_csv}")
        print("✓ Tone gold layer complete!")


if __name__ == "__main__":
    layer = ToneGoldLayer("bitcoin")
    layer.run()
