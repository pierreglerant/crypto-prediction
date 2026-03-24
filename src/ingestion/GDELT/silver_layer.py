#!/usr/bin/env python3
"""
SILVER LAYER: Clean and deduplicate articles from bronze layer.

Output to CSV for further processing
Agnostic to cryptocurrency type
"""
import csv
import json
import os
from datetime import datetime
from urllib.parse import urlparse


class SilverLayer:
    """Clean and deduplicate article data."""

    def __init__(self, coin_name, input_file=None, output_csv=None):
        """
        Args.

        coin_name: Name of cryptocurrency
        input_file: Path to input JSONL (default: {coin_name}_bronze.jsonl)
        output_csv: Path to output CSV (default: {coin_name}_silver.csv)
        """
        self.coin_name = coin_name
        self.input_file = input_file or f"{coin_name}_bronze.jsonl"
        self.output_csv = output_csv or f"{coin_name}_silver.csv"
        self.seen_urls = set()
        self.articles_cleaned = 0
        self.articles_skipped = 0

    def extract_domain(self, url):
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return None

    def is_valid_article(self, article):
        """Validate article has required fields."""
        if not article:
            return False

        required = ['url', 'seendate']
        for field in required:
            if not article.get(field):
                return False

        return True

    def normalize_date(self, date_str):
        """Normalize date to YYYY-MM-DD format."""
        if not date_str:
            return None

        try:
            # Try common formats
            formats = ['%Y-%m-%d', '%Y%m%d', '%Y-%m-%d %H:%M:%S', '%Y%m%d%H%M%S']
            for fmt in formats:
                try:
                    dt = datetime.strptime(str(date_str), fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return date_str
        except Exception:
            return date_str

    def _process_jsonl_input(self, writer):
        """Process JSONL input file."""
        with open(self.input_file, 'r', encoding='utf-8') as infile:
            for line_num, line in enumerate(infile, 1):
                try:
                    article = json.loads(line)

                    # Validate
                    if not self.is_valid_article(article):
                        self.articles_skipped += 1
                        continue

                    url = article['url']

                    # Deduplication
                    if url in self.seen_urls:
                        self.articles_skipped += 1
                        continue

                    self.seen_urls.add(url)

                    # Clean and normalize
                    domain = article.get('domain') or self.extract_domain(url)
                    date_val = self.normalize_date(article.get('seendate', ''))

                    if not date_val:
                        self.articles_skipped += 1
                        continue

                    # Write to CSV
                    writer.writerow({
                        'date': date_val,
                        'url': url,
                        'domain': domain
                    })
                    self.articles_cleaned += 1

                    if self.articles_cleaned % 100000 == 0:
                        print(f"   {self.articles_cleaned:,} articles cleaned...")

                except json.JSONDecodeError:
                    continue

    def _process_csv_input(self, writer):
        """Process CSV input file."""
        with open(self.input_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                url = row.get('url') or row.get('URL') or row.get('DocumentIdentifier')
                date_val = row.get('day') or row.get('date') or row.get('DATE')

                if not url or not date_val:
                    self.articles_skipped += 1
                    continue

                # Deduplication
                if url in self.seen_urls:
                    self.articles_skipped += 1
                    continue

                self.seen_urls.add(url)

                # Clean and normalize
                domain = self.extract_domain(url)
                normalized_date = self.normalize_date(date_val)

                if not normalized_date:
                    self.articles_skipped += 1
                    continue

                # Write to CSV
                writer.writerow({
                    'date': normalized_date,
                    'url': url,
                    'domain': domain
                })
                self.articles_cleaned += 1

                if self.articles_cleaned % 100000 == 0:
                    print(f"   {self.articles_cleaned:,} articles cleaned...")

    def run(self):
        """Execute silver layer cleaning."""
        print(f"🧹 === SILVER LAYER: Clean {self.coin_name.upper()} ===")
        print(f"📥 Input: {self.input_file}")
        print(f"📤 Output: {self.output_csv}")
        print()

        # Determine input format
        is_jsonl = self.input_file.endswith('.jsonl')

        if not os.path.exists(self.input_file):
            print(f"❌ File {self.input_file} not found!")
            return

        try:
            with open(self.output_csv, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(
                    outfile,
                    fieldnames=['date', 'url', 'domain'],
                    extrasaction='ignore'
                )
                writer.writeheader()

                if is_jsonl:
                    self._process_jsonl_input(writer)
                else:
                    self._process_csv_input(writer)

        except Exception as e:
            print(f"❌ Error: {e}")
            return

        print()
        print("📊 === SILVER SUMMARY ===")
        print(f"Articles cleaned: {self.articles_cleaned:,}")
        print(f"Articles rejected: {self.articles_skipped:,}")
        dedup_ratio = (self.articles_skipped / max(1, self.articles_cleaned + self.articles_skipped) * 100)
        print(f"Deduplication rate: {dedup_ratio:.1f}%")
        print(f"Output: {self.output_csv}")
        print("✓ Silver layer complete!")


if __name__ == "__main__":
    coin_name = "bitcoin"

    layer = SilverLayer(coin_name)
    layer.run()
