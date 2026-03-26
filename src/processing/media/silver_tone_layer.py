#!/usr/bin/env python3
"""
SILVER LAYER: Clean and normalize tone records from bronze layer.

Output to CSV for downstream feature engineering.
"""

import csv
import json
import os
from datetime import datetime
from pathlib import Path


class ToneSilverLayer:
    """Clean tone rows and enforce one record per date."""

    def __init__(self, coin_name, input_file=None, output_csv=None):
        """
        Args.

        coin_name: Name of cryptocurrency
        input_file: Path to input JSONL/CSV (default: data/bronze/{coin_name}_tone_bronze.jsonl)
        output_csv: Path to output CSV (default: data/silver/{coin_name}_tone_silver.csv)
        """
        self.coin_name = coin_name
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"
        silver_dir = data_root / "silver"
        silver_dir.mkdir(parents=True, exist_ok=True)

        self.input_file = str(input_file) if input_file else str(data_root / "bronze" / f"{coin_name}_tone_bronze.jsonl")
        self.output_csv = str(output_csv) if output_csv else str(silver_dir / f"{coin_name}_tone_silver.csv")

        self.rows_cleaned = 0
        self.rows_skipped = 0

    def normalize_date(self, date_str):
        """Normalize date to YYYY-MM-DD format."""
        if not date_str:
            return None

        try:
            formats = ["%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"]
            for fmt in formats:
                try:
                    dt = datetime.strptime(str(date_str), fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def _parse_clean_row(self, date_val, avg_tone_raw, article_count_raw):
        """Validate and parse a row from any input format."""
        normalized_date = self.normalize_date(date_val)
        if not normalized_date:
            return None

        try:
            avg_tone = float(avg_tone_raw)
            article_count = int(float(article_count_raw))
        except (TypeError, ValueError):
            return None

        return {
            "date": normalized_date,
            "avg_tone": avg_tone,
            "article_count": article_count,
        }

    def _process_jsonl_input(self):
        """Read tone rows from JSONL."""
        rows = []
        with open(self.input_file, "r", encoding="utf-8") as infile:
            for line in infile:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    self.rows_skipped += 1
                    continue

                parsed = self._parse_clean_row(
                    payload.get("date") or payload.get("day"),
                    payload.get("avg_tone"),
                    payload.get("article_count"),
                )
                if parsed is None:
                    self.rows_skipped += 1
                    continue

                rows.append(parsed)

        return rows

    def _process_csv_input(self):
        """Read tone rows directly from CSV."""
        rows = []
        with open(self.input_file, "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                parsed = self._parse_clean_row(
                    row.get("day") or row.get("date") or row.get("DATE"),
                    row.get("avg_tone"),
                    row.get("article_count"),
                )
                if parsed is None:
                    self.rows_skipped += 1
                    continue

                rows.append(parsed)

        return rows

    def _deduplicate_by_date(self, rows):
        """Keep one row per date using weighted tone when duplicates exist."""
        grouped = {}
        for row in rows:
            date_val = row["date"]
            tone_val = row["avg_tone"]
            count_val = row["article_count"]

            if date_val not in grouped:
                grouped[date_val] = {
                    "weighted_tone_sum": tone_val * count_val,
                    "article_count": count_val,
                }
                continue

            grouped[date_val]["weighted_tone_sum"] += tone_val * count_val
            grouped[date_val]["article_count"] += count_val

        deduped = []
        for date_val in sorted(grouped.keys()):
            total_count = grouped[date_val]["article_count"]
            avg_tone = grouped[date_val]["weighted_tone_sum"] / max(1, total_count)
            deduped.append(
                {
                    "date": date_val,
                    "avg_tone": avg_tone,
                    "article_count": total_count,
                }
            )

        return deduped

    def run(self):
        """Execute tone silver layer cleaning."""
        print(f" === TONE SILVER LAYER: Clean {self.coin_name.upper()} ===")
        print(f" Input: {self.input_file}")
        print(f" Output: {self.output_csv}")
        print()

        if not os.path.exists(self.input_file):
            print(f"❌ File {self.input_file} not found!")
            return

        try:
            is_jsonl = self.input_file.endswith(".jsonl")
            rows = self._process_jsonl_input() if is_jsonl else self._process_csv_input()
            cleaned_rows = self._deduplicate_by_date(rows)

            with open(self.output_csv, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=["date", "avg_tone", "article_count"])
                writer.writeheader()
                for row in cleaned_rows:
                    writer.writerow(row)

            self.rows_cleaned = len(cleaned_rows)

        except Exception as e:
            print(f"❌ Error: {e}")
            return

        print()
        print(" === TONE SILVER SUMMARY ===")
        print(f"Rows cleaned: {self.rows_cleaned:,}")
        print(f"Rows skipped: {self.rows_skipped:,}")
        print(f"Output: {self.output_csv}")
        print("✓ Tone silver layer complete!")


if __name__ == "__main__":
    layer = ToneSilverLayer("bitcoin")
    layer.run()
