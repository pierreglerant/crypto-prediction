#!/usr/bin/env python3
"""
BRONZE LAYER: Load articles from CSV and optionally fetch missing dates from GDELT.

Agnostic to cryptocurrency type
"""

import csv
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import requests


class BronzeLayer:
    """Load and enrich articles from CSV source with optional GDELT fetch."""

    def __init__(self, coin_name, query_terms, input_csv=None, output_jsonl=None):
        """
        Args.

        coin_name: Name of cryptocurrency (bitcoin, ethereum, etc.)
        query_terms: List of search terms (["bitcoin", "btc"])
        input_csv: Path to input CSV (default: data/cache/bigquery/{coin_name}_articles_bigquery.csv)
        output_jsonl: Path to output JSONL (default: data/bronze/{coin_name}_bronze.jsonl)
        """
        self.coin_name = coin_name
        self.query_terms = query_terms
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

    def get_max_date_from_csv(self):
        """Extract max date from CSV."""
        try:
            with open(self.input_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                dates = []
                for row in reader:
                    if row.get("day") or row.get("date"):
                        date_val = row.get("day") or row.get("date")
                        dates.append(date_val)
                if dates:
                    return max(dates)
            return None
        except FileNotFoundError:
            return None

    def load_csv_to_jsonl(self):
        """Load CSV and convert to enriched JSONL format."""
        print(f" === BRONZE LAYER: Load {self.coin_name.upper()} ===")
        print(f" Input: {self.input_csv}")
        print(f" Output: {self.output_jsonl}")
        print()

        if not os.path.exists(self.input_csv):
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

    def _build_query_string(self):
        """Build GDELT query string from search terms."""
        return f"({' OR '.join(self.query_terms)})"

    def _process_date_range(self, start_date, end_date, query):
        """Process articles for a specific date range."""
        start_dt = f"{start_date.year}{start_date.month:02d}{start_date.day:02d}000000"
        end_dt = f"{end_date.year}{end_date.month:02d}{end_date.day:02d}235959"

        time.sleep(self.min_request_interval)

        print(f" {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}...", end=" ", flush=True)

        try:
            params = {
                "query": query,
                "mode": "artlist",
                "format": "json",
                "maxrecords": 250,
                "STARTDATETIME": start_dt,
                "ENDDATETIME": end_dt,
            }

            response = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=60)

            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])

                # Append to JSONL
                with open(self.output_jsonl, "a", encoding="utf-8") as f:
                    for article in articles:
                        article["seendate"] = start_date.strftime("%Y-%m-%d")
                        domain = article.get("domain", "").lower()
                        if domain.startswith("www."):
                            domain = domain[4:]
                        article["domain"] = domain
                        f.write(json.dumps(article, ensure_ascii=False) + "\n")
                        self.articles_fetched += 1

                print(f"✓ {len(articles)} articles")
                return len(articles)

            elif response.status_code == 429:
                print("⚠️  Rate limited, waiting 60s...")
                time.sleep(60)
                return 0
            else:
                print(f"⚠️  HTTP {response.status_code}")
                return 0

        except Exception as e:
            print(f"❌ Error: {str(e)[:80]}")
            return 0

    def fetch_missing_dates(self):
        """Optionally fetch articles from GDELT for dates after CSV max date."""
        max_date = self.get_max_date_from_csv()

        if not max_date:
            print("⚠️ Unable to determine max date from CSV")
            return

        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
        today = datetime.now()

        if max_date_obj >= today:
            print(f"✓ CSV already up-to-date (max date: {max_date})")
            return

        print(f"\n Fetch missing dates: {max_date} → {today.strftime('%Y-%m-%d')}")
        print()

        # Build query string
        query = self._build_query_string()

        # Temporal pagination
        chunk_days = 7
        current_start = max_date_obj + timedelta(days=1)

        try:
            while current_start < today:
                current_end = min(current_start + timedelta(days=chunk_days), today)

                article_count = self._process_date_range(current_start, current_end, query)

                # Adaptive chunk sizing
                if article_count >= 240:
                    chunk_days = max(1, chunk_days // 2)
                elif article_count < 50 and chunk_days < 30:
                    chunk_days = min(30, chunk_days * 2)

                current_start = current_end

        except KeyboardInterrupt:
            print("\n\n🛑 Interrupted!")

    def run(self, fetch_missing=False):
        """Execute bronze layer."""
        success = self.load_csv_to_jsonl()

        if success and fetch_missing:
            self.fetch_missing_dates()

        print("\n === BRONZE SUMMARY ===")
        print(f"Articles loaded from CSV: {self.articles_loaded:,}")
        print(f"Articles fetched from GDELT: {self.articles_fetched:,}")
        print(f"Total: {self.articles_loaded + self.articles_fetched:,}")
        print(f"Output: {self.output_jsonl}")
        print("✓ Bronze layer complete!")


if __name__ == "__main__":
    import sys

    coin_name = "bitcoin"
    query_terms = ["bitcoin", "btc"]
    fetch_missing = len(sys.argv) > 1 and sys.argv[1] == "--fetch"

    layer = BronzeLayer(coin_name, query_terms)
    layer.run(fetch_missing=fetch_missing)
