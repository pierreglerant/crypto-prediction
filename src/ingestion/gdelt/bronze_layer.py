#!/usr/bin/env python3
"""
BRONZE LAYER: Load articles from CSV or backfill them from BigQuery.

Agnostic to cryptocurrency type.
"""

import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from src.config.gcp import build_daily_article_cache_query, load_bigquery_settings

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


class BronzeLayer:
    """Load and enrich articles from CSV source with optional BigQuery backfill."""

    def __init__(self, coin_name, query_terms, input_csv=None, output_jsonl=None):
        """
        Args.

        coin_name: Name of cryptocurrency (bitcoin, ethereum, etc.)
        query_terms: List of search terms (["bitcoin", "btc"])
        input_csv: Path to input CSV (default: data/cache/bigquery/{coin_name}_articles_bigquery.csv)
        output_jsonl: Path to output JSONL (default: data/bronze/{coin_name}_bronze.jsonl)
        """
        self.coin_name = coin_name
        self.query_terms = list(query_terms)
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"

        default_cache_dir = data_root / "cache" / "bigquery"
        fallback_cache_dir = data_root / "cache"
        bronze_dir = data_root / "bronze"
        bronze_dir.mkdir(parents=True, exist_ok=True)

        default_input = default_cache_dir / f"{coin_name}_articles_bigquery.csv"
        fallback_input = fallback_cache_dir / f"{coin_name}_articles_bigquery.csv"

        if input_csv:
            self.input_csv = str(input_csv)
        elif default_input.exists():
            self.input_csv = str(default_input)
        elif fallback_input.exists():
            self.input_csv = str(fallback_input)
        else:
            self.input_csv = str(default_input)

        self.output_jsonl = str(output_jsonl) if output_jsonl else str(bronze_dir / f"{coin_name}_bronze.jsonl")
        self.min_request_interval = 5.0
        self.articles_loaded = 0
        self.articles_fetched = 0

    def _normalize_day(self, value):
        """Normalize a day value to YYYY-MM-DD."""
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d%H%M%S"):
            try:
                return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

        if len(text) >= 8 and text[:8].isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"

        return text[:10]

    def get_max_date_from_csv(self):
        """Extract max date from CSV."""
        if not os.path.exists(self.input_csv) or os.path.getsize(self.input_csv) == 0:
            return None

        try:
            with open(self.input_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                dates = []
                for row in reader:
                    if row.get("day") or row.get("date"):
                        date_val = row.get("day") or row.get("date")
                        normalized = self._normalize_day(date_val)
                        if normalized:
                            dates.append(normalized)
                if dates:
                    return max(dates)
            return None
        except FileNotFoundError:
            return None

    def _bigquery_client(self):
        """Create a BigQuery client using explicit credentials or ADC."""
        settings = load_bigquery_settings(enabled=True)

        if bigquery is None:
            print("⚠️ BigQuery client unavailable; install google-cloud-bigquery to enable backfill.")
            return None, settings

        try:
            if google_auth is None:
                raise RuntimeError("google-auth is not installed")

            if settings.credentials_path:
                credentials, detected_project = google_auth.load_credentials_from_file(
                    str(settings.credentials_path),
                    scopes=["https://www.googleapis.com/auth/bigquery"],
                )
            else:
                credentials, detected_project = google_auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])

            project_id = settings.project_id or detected_project
            client = bigquery.Client(credentials=credentials, project=project_id, location=settings.location)
            return client, settings
        except Exception as exc:
            print(f"⚠️ BigQuery client initialization failed: {exc}")
            return None, settings

    def _row_value(self, row, *candidates):
        """Extract a value from a BigQuery row using several possible field names."""
        for candidate in candidates:
            value = getattr(row, candidate, None)
            if value is not None:
                return value

        return None

    def _query_bigquery_articles(self, target_path: Path, start_date: str | None = None, end_date: str | None = None, append: bool = False) -> bool:
        """Materialize article rows from BigQuery into the local cache CSV."""
        client, settings = self._bigquery_client()
        if client is None:
            return False

        query = build_daily_article_cache_query(self.query_terms, start_date=start_date, end_date=end_date)

        mode = "a" if append and target_path.exists() and os.path.getsize(target_path) > 0 else "w"
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            query_job = client.query(query, location=settings.location)
            rows = query_job.result()

            file_exists = target_path.exists() and os.path.getsize(target_path) > 0
            with open(target_path, mode, newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["day", "url"])
                if mode == "w" or not file_exists:
                    writer.writeheader()

                written = 0
                for row in rows:
                    day = self._row_value(row, "day")
                    url = self._row_value(row, "url")

                    if not day or not url:
                        continue

                    writer.writerow({"day": self._normalize_day(day), "url": url})
                    written += 1

        except Exception as exc:
            print(f"❌ BigQuery article backfill failed: {exc}")
            return False

        if written == 0:
            print("⚠️ BigQuery returned no article rows")
            return False

        self.articles_fetched += written
        print(f"✓ BigQuery cached {written:,} articles to {target_path}")
        return True

    def load_csv_to_jsonl(self):
        """Load CSV and convert to enriched JSONL format."""
        print(f" === BRONZE LAYER: Load {self.coin_name.upper()} ===")
        print(f" Input: {self.input_csv}")
        print(f" Output: {self.output_jsonl}")
        print()

        if not os.path.exists(self.input_csv) or os.path.getsize(self.input_csv) == 0:
            print(f"❌ File {self.input_csv} not found!")
            return False

        # Reset output file
        with open(self.output_jsonl, "w", encoding="utf-8"):
            pass

        try:
            with open(self.input_csv, "r", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)

                with open(self.output_jsonl, "a", encoding="utf-8") as outfile:
                    for row in reader:
                        url = row.get("url") or row.get("URL") or row.get("DocumentIdentifier")
                        date_val = row.get("day") or row.get("date") or row.get("DATE")

                        if not url or not date_val:
                            continue

                        # Extract domain
                        try:
                            domain = urlparse(url).netloc.lower()
                            if domain.startswith("www."):
                                domain = domain[4:]
                        except Exception:
                            domain = ""

                        # Create enriched article
                        article = {"url": url, "seendate": date_val, "domain": domain}

                        outfile.write(json.dumps(article, ensure_ascii=False) + "\n")
                        self.articles_loaded += 1

                        if self.articles_loaded % 100000 == 0:
                            print(f"   ✓ {self.articles_loaded:,} articles loaded...")

            print(f"✓ {self.articles_loaded:,} articles loaded from CSV")
            return True

        except Exception as e:
            print(f"❌ Error loading CSV: {e}")
            return False

    def fetch_missing_dates(self):
        """Backfill missing article dates from BigQuery."""
        max_date = self.get_max_date_from_csv()

        if not max_date:
            print("⚠️ Unable to determine max date from CSV; refreshing full cache from BigQuery instead.")
            self._query_bigquery_articles(Path(self.input_csv), append=False)
            return

        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d")

        if max_date_obj.strftime("%Y-%m-%d") >= today:
            print(f"✓ CSV already up-to-date (max date: {max_date})")
            return

        print(f"\n Backfill missing dates from BigQuery: {max_date} → {today}")
        print()

        start_date = (max_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
        self._query_bigquery_articles(Path(self.input_csv), start_date=start_date, end_date=today, append=True)

    def _ensure_cache_from_bigquery(self, fetch_missing: bool = False):
        """Create or refresh the local article cache using BigQuery only."""
        cache_path = Path(self.input_csv)

        if not cache_path.exists() or os.path.getsize(cache_path) == 0:
            print(f"⚠️ Seed cache missing, querying BigQuery for {self.coin_name}...")
            return self._query_bigquery_articles(cache_path, append=False)

        if fetch_missing:
            self.fetch_missing_dates()

        return True

    def run(self, fetch_missing=False):
        """Execute bronze layer."""
        if not self._ensure_cache_from_bigquery(fetch_missing=fetch_missing):
            print(f"❌ Unable to materialize cache for {self.input_csv}")
            return

        self.load_csv_to_jsonl()

        print("\n === BRONZE SUMMARY ===")
        print(f"Articles loaded from CSV: {self.articles_loaded:,}")
        print(f"Articles fetched into cache from BigQuery: {self.articles_fetched:,}")
        print(f"Total: {self.articles_loaded:,}")
        print(f"Output: {self.output_jsonl}")
        print("✓ Bronze layer complete!")


if __name__ == "__main__":
    import sys

    coin_name = "bitcoin"
    query_terms = ["bitcoin", "btc"]
    fetch_missing = len(sys.argv) > 1 and sys.argv[1] == "--fetch"

    layer = BronzeLayer(coin_name, query_terms)
    layer.run(fetch_missing=fetch_missing)
