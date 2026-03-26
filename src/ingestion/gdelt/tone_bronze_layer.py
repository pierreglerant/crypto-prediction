#!/usr/bin/env python3
"""
BRONZE LAYER: Load daily tone metrics from CSV to JSONL.

Agnostic to cryptocurrency type
"""

import csv
import json
import os
from pathlib import Path


class ToneBronzeLayer:
    """Load tone count data from CSV and normalize basic schema."""

    def __init__(self, coin_name, input_csv=None, output_jsonl=None):
        """
        Args.

        coin_name: Name of cryptocurrency (bitcoin, ethereum, etc.)
        input_csv: Path to input CSV (default: data/cache/{coin_name}_tone_count_1d.csv)
        output_jsonl: Path to output JSONL (default: data/bronze/{coin_name}_tone_bronze.jsonl)
        """
        self.coin_name = coin_name
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"

        bronze_dir = data_root / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        default_input = data_root / "cache" / f"{coin_name}_tone_count_1d.csv"

        self.input_csv = str(input_csv) if input_csv else str(default_input)
        self.output_jsonl = str(output_jsonl) if output_jsonl else str(bronze_dir / f"{coin_name}_tone_bronze.jsonl")

        self.rows_loaded = 0
        self.rows_skipped = 0

    def _parse_row(self, row):
        """Validate and normalize a raw CSV row."""
        day = row.get("day") or row.get("date") or row.get("DATE")
        avg_tone_raw = row.get("avg_tone")
        article_count_raw = row.get("article_count")

        if not day or avg_tone_raw is None or article_count_raw is None:
            return None

        try:
            avg_tone = float(avg_tone_raw)
            article_count = int(float(article_count_raw))
        except (TypeError, ValueError):
            return None

        return {
            "date": str(day),
            "avg_tone": avg_tone,
            "article_count": article_count,
        }

    def run(self):
        """Execute tone bronze layer."""
        print(f" === TONE BRONZE LAYER: Load {self.coin_name.upper()} ===")
        print(f" Input: {self.input_csv}")
        print(f" Output: {self.output_jsonl}")
        print()

        if not os.path.exists(self.input_csv):
            print(f"❌ File {self.input_csv} not found!")
            return

        # Reset output file.
        with open(self.output_jsonl, "w", encoding="utf-8"):
            pass

        try:
            with open(self.input_csv, "r", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)

                with open(self.output_jsonl, "a", encoding="utf-8") as outfile:
                    for row in reader:
                        parsed = self._parse_row(row)
                        if parsed is None:
                            self.rows_skipped += 1
                            continue

                        outfile.write(json.dumps(parsed, ensure_ascii=False) + "\n")
                        self.rows_loaded += 1

        except Exception as e:
            print(f"❌ Error loading tone CSV: {e}")
            return

        print()
        print(" === TONE BRONZE SUMMARY ===")
        print(f"Rows loaded: {self.rows_loaded:,}")
        print(f"Rows skipped: {self.rows_skipped:,}")
        print(f"Output: {self.output_jsonl}")
        print("✓ Tone bronze layer complete!")


if __name__ == "__main__":
    layer = ToneBronzeLayer("bitcoin")
    layer.run()
