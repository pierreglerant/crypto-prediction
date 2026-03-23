"""Integration tests for the BTC data engineering pipeline."""

import json

import pandas as pd
import pytest
from conftest import make_raw_klines

from pipelines import btc as btc_pipeline
from utils import io as io_utils

pytestmark = pytest.mark.integration


def test_btc_pipeline_writes_bronze_silver_and_gold_outputs(monkeypatch, tmp_path) -> None:
    """The BTC pipeline should write all expected artifacts end to end."""
    raw_data = make_raw_klines(50)

    monkeypatch.setattr(btc_pipeline, "fetch_full_history", lambda *args: raw_data)
    monkeypatch.setattr(io_utils, "BASE_DIR", tmp_path)

    btc_pipeline.main()

    bronze_path = tmp_path / "data/bronze/market/btc_usdt_1d_raw.json"
    silver_path = tmp_path / "data/silver/market/btc_usdt_1d.csv"
    gold_path = tmp_path / "data/gold/market/btc_usdt_1d_features.csv"

    assert bronze_path.exists()
    assert silver_path.exists()
    assert gold_path.exists()

    saved_raw = json.loads(bronze_path.read_text())
    silver_df = pd.read_csv(silver_path)
    gold_df = pd.read_csv(gold_path)

    assert saved_raw == raw_data
    assert len(silver_df) == len(raw_data)
    assert "target" in gold_df.columns
    assert len(gold_df) > 0
