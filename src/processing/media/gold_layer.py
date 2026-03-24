#!/usr/bin/env python3
"""
GOLD LAYER: Aggregate articles by date and source domain.

Output final dataset with article counts per day/source
Agnostic to cryptocurrency type
"""

import csv
import os
from collections import defaultdict
from pathlib import Path


class GoldLayer:
    """Aggregate and summarize article data by date and source."""

    def __init__(self, coin_name, input_csv=None, output_csv=None, source_mappings=None):
        """
        Args.

        coin_name: Name of cryptocurrency
        input_csv: Path to input CSV (default: data/silver/{coin_name}_silver.csv)
        output_csv: Path to output CSV (default: data/gold/{coin_name}_gold.csv)
        source_mappings: Dict mapping domain → source name
        """
        self.coin_name = coin_name
        repo_root = Path(__file__).resolve().parents[3]
        data_root = repo_root / "data"
        gold_dir = data_root / "gold"
        gold_dir.mkdir(parents=True, exist_ok=True)

        self.input_csv = str(input_csv) if input_csv else str(data_root / "silver" / f"{coin_name}_silver.csv")
        self.output_csv = str(output_csv) if output_csv else str(gold_dir / f"{coin_name}_gold.csv")

        # Default source mappings (can be overridden)
        self.source_mappings = source_mappings or {
            "cnn.com": "CNN",
            "bbc.com": "BBC",
            "bbc.co.uk": "BBC",
            "reuters.com": "Reuters",
            "bloomberg.com": "Bloomberg",
            "ft.com": "Financial Times",
            "nytimes.com": "New York Times",
            "lemonde.fr": "Le Monde",
            "lesechos.fr": "Les Échos",
            "france24.com": "France 24",
            "lefigaro.fr": "Le Figaro",
            # Crypto outlets
            "coindesk.com": "CoinDesk",
            "coinspeaker.com": "CoinSpeaker",
            "cointelegraph.com": "Cointelegraph",
            "bitcoin.com": "Bitcoin.com",
            "decrypt.co": "Decrypt",
        }

        self.data = defaultdict(lambda: defaultdict(int))  # day -> source -> count
        self.total_articles = 0

    def get_source_name(self, domain):
        """Map domain to source name."""
        if not domain:
            return "Other"

        domain = domain.lower()

        # Exact match
        if domain in self.source_mappings:
            return self.source_mappings[domain]

        # Partial match
        for mapped_domain, source in self.source_mappings.items():
            if mapped_domain in domain or domain in mapped_domain:
                return source

        return "Other"

    def _load_and_aggregate_data(self):
        """Load CSV data and aggregate by date and source."""
        try:
            with open(self.input_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for row in reader:
                    try:
                        date_val = row.get("date", "")
                        domain = row.get("domain", "")

                        if not date_val or not domain:
                            continue

                        # Get source name
                        source = self.get_source_name(domain)

                        # Count
                        self.data[date_val][source] += 1
                        self.total_articles += 1

                        if self.total_articles % 100000 == 0:
                            print(f"   Aggregated {self.total_articles:,} articles...")

                    except Exception:
                        continue

        except Exception as e:
            print(f"❌ Error reading input: {e}")
            return False
        return True

    def _write_aggregated_csv(self):
        """Write aggregated data to output CSV."""
        print()
        print(" Writing aggregated CSV...")

        # Get all unique sources
        all_sources = set()
        for day_data in self.data.values():
            all_sources.update(day_data.keys())

        all_sources = sorted(list(all_sources))

        # Write output
        fieldnames = ["date"] + all_sources + ["total"]

        try:
            with open(self.output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                # Write sorted by date
                for date_val in sorted(self.data.keys()):
                    row = {"date": date_val}
                    day_total = 0

                    for source in all_sources:
                        count = self.data[date_val][source]
                        row[source] = count if count > 0 else 0
                        day_total += count

                    row["total"] = day_total
                    writer.writerow(row)

        except Exception as e:
            print(f"❌ Error writing output: {e}")
            return None
        return all_sources

    def run(self):
        """Execute gold layer aggregation."""
        print(f" === GOLD LAYER: Aggregate {self.coin_name.upper()} ===")
        print(f" Input: {self.input_csv}")
        print(f" Output: {self.output_csv}")
        print()

        if not os.path.exists(self.input_csv):
            print(f"❌ File {self.input_csv} not found!")
            return

        # Load and aggregate data
        if not self._load_and_aggregate_data():
            return

        # Write aggregated CSV
        all_sources = self._write_aggregated_csv()
        if all_sources is None:
            return

        print()
        print(" === GOLD SUMMARY ===")
        print(f"Articles aggregated: {self.total_articles:,}")
        print(f"Unique days: {len(self.data):,}")
        print(f"Unique sources: {len(all_sources)}")
        if all_sources:
            sources_preview = ", ".join(all_sources[:10])
            if len(all_sources) > 10:
                sources_preview += f"... (+{len(all_sources) - 10} more)"
            print(f"  {sources_preview}")
        print(f"Output: {self.output_csv}")
        print("✓ Gold layer complete!")


if __name__ == "__main__":
    coin_name = "bitcoin"

    layer = GoldLayer(coin_name)
    layer.run()
