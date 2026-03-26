#!/usr/bin/env python3
"""
MERGE LAYER: Combine media gold data with enriched tone gold features.

Output preserves all media-gold dates and appends tone fields.
"""

import csv
import os
from pathlib import Path


class GoldToneMergeLayer:
    """Left-join media gold and tone gold on date."""

    def __init__(self, coin_name, media_gold_csv=None, tone_gold_csv=None, output_csv=None):
        """
        Args.

        coin_name: Name of cryptocurrency
        media_gold_csv: Path to media gold CSV (default: data/gold/{coin_name}_gold.csv)
        tone_gold_csv: Path to tone gold CSV (default: data/gold/{coin_name}_tone_gold.csv)
        output_csv: Path to merged output (default: data/gold/{coin_name}_gold_with_tone.csv)
        """
        self.coin_name = coin_name
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"
        gold_dir = data_root / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)

        self.media_gold_csv = str(media_gold_csv) if media_gold_csv else str(gold_dir / f"{coin_name}_gold.csv")
        self.tone_gold_csv = str(tone_gold_csv) if tone_gold_csv else str(gold_dir / f"{coin_name}_tone_gold.csv")
        self.output_csv = str(output_csv) if output_csv else str(gold_dir / f"{coin_name}_gold_with_tone.csv")

        self.rows_written = 0

    def _load_tone_index(self):
        """Load tone rows indexed by date."""
        tone_by_date = {}

        with open(self.tone_gold_csv, "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                date_val = row.get("date")
                if not date_val:
                    continue
                tone_by_date[date_val] = row

        return tone_by_date

    def run(self):
        """Execute media+tone merge."""
        print(f" === MERGE LAYER: MEDIA + TONE {self.coin_name.upper()} ===")
        print(f" Media gold input: {self.media_gold_csv}")
        print(f" Tone gold input: {self.tone_gold_csv}")
        print(f" Output: {self.output_csv}")
        print()

        if not os.path.exists(self.media_gold_csv):
            print(f"❌ File {self.media_gold_csv} not found!")
            return

        if not os.path.exists(self.tone_gold_csv):
            print(f"❌ File {self.tone_gold_csv} not found!")
            return

        try:
            tone_by_date = self._load_tone_index()

            with open(self.media_gold_csv, "r", encoding="utf-8") as media_in:
                media_reader = csv.DictReader(media_in)
                media_fieldnames = media_reader.fieldnames or []

                tone_fields = [
                    "avg_tone",
                    "article_count",
                    "tone_ma_3",
                    "tone_ma_7",
                    "tone_lag_1",
                    "tone_lag_3",
                    "article_count_ma_3",
                    "article_count_lag_1",
                ]

                merged_fieldnames = list(media_fieldnames)
                for field in tone_fields:
                    if field not in merged_fieldnames:
                        merged_fieldnames.append(field)

                with open(self.output_csv, "w", newline="", encoding="utf-8") as outfile:
                    writer = csv.DictWriter(outfile, fieldnames=merged_fieldnames)
                    writer.writeheader()

                    for media_row in media_reader:
                        date_val = media_row.get("date")
                        tone_row = tone_by_date.get(date_val, {})

                        merged_row = dict(media_row)
                        for field in tone_fields:
                            merged_row[field] = tone_row.get(field, "")

                        writer.writerow(merged_row)
                        self.rows_written += 1

        except Exception as e:
            print(f"❌ Error: {e}")
            return

        print()
        print(" === MERGE SUMMARY ===")
        print(f"Rows written: {self.rows_written:,}")
        print(f"Output: {self.output_csv}")
        print("✓ Merge layer complete!")


if __name__ == "__main__":
    layer = GoldToneMergeLayer("bitcoin")
    layer.run()
