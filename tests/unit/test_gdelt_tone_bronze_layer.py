"""Tests for the tone bronze layer and GCP query placeholders."""

from __future__ import annotations

from config.gcp import build_daily_tone_count_query
from ingestion.gdelt.tone_bronze_layer import ToneBronzeLayer


def test_tone_bronze_layer_prefers_local_tone_gold() -> None:
    """Default tone input should use the local cache CSV."""
    layer = ToneBronzeLayer("bitcoin")

    assert layer.input_csv.endswith("/data/cache/bitcoin_tone_count_1d.csv")
    assert "bitcoin_tone_count_1d.csv" in layer.input_csv


def test_build_daily_tone_count_query_filters_by_coin() -> None:
    """The future BigQuery SQL should be parameterized by coin name."""
    query = build_daily_tone_count_query("bitcoin")

    assert "COUNT(*) AS nb_articles" in query
    assert "AVG(SAFE_CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) AS avg_tone" in query
    assert "LOWER(V2Themes) LIKE '%bitcoin%'" in query
