"""Unit tests for the GDELT gold layer."""

from __future__ import annotations

import csv

from processing.media.gold_layer import GoldLayer


def test_gold_layer_aggregates_by_source(tmp_path) -> None:
    """`GoldLayer` should map domains to sources and aggregate counts."""
    input_csv = tmp_path / "bitcoin_silver.csv"
    output_csv = tmp_path / "bitcoin_gold.csv"

    with input_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "url", "domain"])
        writer.writeheader()
        writer.writerow({"date": "2024-01-01", "url": "https://www.reuters.com/a", "domain": "reuters.com"})
        writer.writerow({"date": "2024-01-01", "url": "https://news.example.com/b", "domain": "news.example.com"})
        writer.writerow({"date": "2024-01-02", "url": "https://news.example.com/c", "domain": "news.example.com"})

    layer = GoldLayer("bitcoin", input_csv=input_csv, output_csv=output_csv)
    layer.run()

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows == [
        {"date": "2024-01-01", "Other": "1", "Reuters": "1", "total": "2"},
        {"date": "2024-01-02", "Other": "1", "Reuters": "0", "total": "1"},
    ]
    assert layer.total_articles == 3
