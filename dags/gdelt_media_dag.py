"""Airflow DAG for GDELT media pipeline (bronze -> silver -> gold)."""

from config.coins import BTC_GDELT
from ingestion.gdelt.bronze_layer import BronzeLayer
from ingestion.gdelt.tone_bronze_layer import ToneBronzeLayer
from pipelines.airflow_dag_factory import build_gdelt_dag
from processing.media.gold_layer import GoldLayer
from processing.media.gold_tone_layer import ToneGoldLayer
from processing.media.gold_tone_merge_layer import GoldToneMergeLayer
from processing.media.silver_layer import SilverLayer
from processing.media.silver_tone_layer import ToneSilverLayer

dag = build_gdelt_dag(
    config=BTC_GDELT,
    BronzeLayer=BronzeLayer,
    SilverLayer=SilverLayer,
    GoldLayer=GoldLayer,
    ToneBronzeLayer=ToneBronzeLayer,
    ToneSilverLayer=ToneSilverLayer,
    ToneGoldLayer=ToneGoldLayer,
    GoldToneMergeLayer=GoldToneMergeLayer,
)
