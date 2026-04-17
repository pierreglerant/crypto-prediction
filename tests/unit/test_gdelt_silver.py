"""Unit tests for the GDELT silver layer."""

from __future__ import annotations

import csv

from processing.media.silver_layer import SilverLayer


def test_silver_layer_deduplicates_and_normalizes_jsonl(tmp_path) -> None:
    """`SilverLayer` should deduplicate URLs and normalize dates."""
    input_jsonl = tmp_path / "bitcoin_bronze.jsonl"
    output_csv = tmp_path / "bitcoin_silver.csv"

    input_jsonl.write_text(
        "\n".join(
            [
                '{"url": "https://www.reuters.com/a", "seendate": "20240102", "domain": "reuters.com"}',
                '{"url": "https://www.reuters.com/a", "seendate": "2024-01-03", "domain": "reuters.com"}',
                '{"url": "https://news.example.com/b", "seendate": "2024-01-04 12:00:00", "domain": "news.example.com"}',
            ]
        ),
        encoding="utf-8",
    )

    layer = SilverLayer("bitcoin", input_file=input_jsonl, output_csv=output_csv)
    layer.run()

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows == [
        {"date": "2024-01-02", "url": "https://www.reuters.com/a", "domain": "reuters.com"},
        {"date": "2024-01-04", "url": "https://news.example.com/b", "domain": "news.example.com"},
    ]
    assert layer.articles_cleaned == 2
    assert layer.articles_skipped == 1
