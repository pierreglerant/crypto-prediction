"""Unit tests for the GDELT bronze layer."""

from __future__ import annotations

import csv
import json

from ingestion.gdelt.bronze_layer import BronzeLayer


def test_bronze_layer_loads_csv_to_jsonl(tmp_path) -> None:
    """`BronzeLayer` should enrich CSV rows into JSONL records."""
    input_csv = tmp_path / "bitcoin_articles.csv"
    output_jsonl = tmp_path / "bitcoin_bronze.jsonl"

    with input_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["url", "date"])
        writer.writeheader()
        writer.writerow({"url": "https://www.reuters.com/world/article", "date": "2024-01-01"})
        writer.writerow({"url": "https://news.example.com/story", "date": "2024-01-02"})

    layer = BronzeLayer("bitcoin", ["bitcoin", "btc"], input_csv=input_csv, output_jsonl=output_jsonl)

    assert layer.load_csv_to_jsonl() is True

    records = [json.loads(line) for line in output_jsonl.read_text().splitlines()]

    assert records == [
        {"url": "https://www.reuters.com/world/article", "seendate": "2024-01-01", "domain": "reuters.com"},
        {"url": "https://news.example.com/story", "seendate": "2024-01-02", "domain": "news.example.com"},
    ]
    assert layer.articles_loaded == 2
