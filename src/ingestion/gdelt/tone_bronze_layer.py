#!/usr/bin/env python3
"""
BRONZE LAYER: Load daily tone metrics from a local CSV to JSONL.

The layer is local-first: it uses the cache CSV by default and only falls back
to BigQuery when the cache is missing. When BigQuery is used, the result is
written back to the cache in the same local CSV format so the rest of the
pipeline keeps the same architecture.
"""

import csv
import json
import os
from pathlib import Path

from src.config.gcp import build_daily_tone_count_query

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional dependency for local mode
    bigquery = None

try:
    import google.auth
except ImportError:  # pragma: no cover - optional dependency for local mode
    google_auth = None
else:
    google_auth = google.auth


class ToneBronzeLayer:
    """Load daily tone/count data from CSV and normalize basic schema."""

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
        cache_dir = data_root / "cache"
        bronze_dir.mkdir(parents=True, exist_ok=True)
        cache_dir.mkdir(parents=True, exist_ok=True)

        self.cache_csv = cache_dir / f"{coin_name}_tone_count_1d.csv"

        self.bigquery_query = build_daily_tone_count_query(coin_name)
        self.default_input_candidates = [self.cache_csv]

        if input_csv:
            print("⚠️ tone_bronze input_csv is deprecated; using the 1d cache file instead.")

        self.input_csv = str(self.cache_csv)

        self.output_jsonl = str(output_jsonl) if output_jsonl else str(bronze_dir / f"{coin_name}_tone_bronze.jsonl")

        self.rows_loaded = 0
        self.rows_skipped = 0

    def _resolve_input_path(self) -> Path:
        """Return the 1d cache CSV or populate it from BigQuery if needed."""
        input_path = Path(self.input_csv)
        if input_path.exists():
            return input_path

        for candidate in self.default_input_candidates:
            if candidate.exists():
                return candidate

        if self._fetch_from_bigquery(self.cache_csv):
            return self.cache_csv

        return input_path

    def _fetch_from_bigquery(self, cache_path: Path) -> bool:
        """Fetch tone/count rows from BigQuery and persist them as a local CSV cache.

        The cache is written in the local schema expected by the pipeline:
        `day`, `avg_tone`, `article_count`.
        """
        if bigquery is None:
            print("⚠️ BigQuery client unavailable; install google-cloud-bigquery to enable fallback.")
            return False

        try:
            settings = {
                "project_id": os.getenv("GOOGLE_CLOUD_PROJECT", "").strip() or None,
                "location": os.getenv("BIGQUERY_LOCATION", "EU").strip() or "EU",
                "credentials_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip() or None,
            }

            if google_auth is None:
                raise RuntimeError("google-auth is not installed")

            if settings["credentials_path"]:
                credentials, detected_project = google_auth.load_credentials_from_file(
                    settings["credentials_path"],
                    scopes=["https://www.googleapis.com/auth/bigquery"],
                )
            else:
                credentials, detected_project = google_auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])

            project_id = settings["project_id"] or detected_project
            client = bigquery.Client(credentials=credentials, project=project_id, location=settings["location"])
        except Exception as exc:
            print(f"⚠️ BigQuery client initialization failed: {exc}")
            return False

        print("⚠️ Cache CSV missing, querying BigQuery as fallback (cost may apply)...")

        try:
            query_job = client.query(self.bigquery_query, location=settings["location"])
            rows = query_job.result()
            with open(cache_path, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=["day", "avg_tone", "article_count"])
                writer.writeheader()
                for row in rows:
                    day = getattr(row, "jour", None)
                    avg_tone = getattr(row, "avg_tone", None)
                    article_count = getattr(row, "nb_articles", None)

                    if day is None or avg_tone is None or article_count is None:
                        continue

                    writer.writerow(
                        {
                            "day": str(day),
                            "avg_tone": avg_tone,
                            "article_count": article_count,
                        }
                    )
        except Exception as exc:
            print(f"❌ BigQuery fallback failed: {exc}")
            return False

        print(f"✓ BigQuery fallback cached to {cache_path}")
        return cache_path.exists()

    def _parse_row(self, row):
        """Validate and normalize a raw CSV row."""
        day = row.get("day") or row.get("date") or row.get("DATE") or row.get("jour")
        avg_tone_raw = row.get("avg_tone")
        article_count_raw = row.get("article_count") or row.get("nb_articles") or row.get("count")

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

        resolved_input = self._resolve_input_path()
        if not resolved_input.exists():
            print(f"❌ File {self.input_csv} not found!")
            return

        self.input_csv = str(resolved_input)

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
