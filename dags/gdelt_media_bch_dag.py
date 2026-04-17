"""Airflow DAG for Bitcoin Cash GDELT media pipeline (bronze -> silver -> gold)."""

from src.config.coins import BCH_GDELT
from src.ingestion.gdelt.bronze_layer import BronzeLayer
from src.ingestion.gdelt.tone_bronze_layer import ToneBronzeLayer
from src.pipelines.airflow_dag_factory import build_gdelt_dag
from src.processing.media.gold_layer import GoldLayer
from src.processing.media.gold_tone_layer import ToneGoldLayer
from src.processing.media.gold_tone_merge_layer import GoldToneMergeLayer
from src.processing.media.silver_layer import SilverLayer
from src.processing.media.silver_tone_layer import ToneSilverLayer

dag = build_gdelt_dag(
    config=BCH_GDELT,
    BronzeLayer=BronzeLayer,
    SilverLayer=SilverLayer,
    GoldLayer=GoldLayer,
    ToneBronzeLayer=ToneBronzeLayer,
    ToneSilverLayer=ToneSilverLayer,
    ToneGoldLayer=ToneGoldLayer,
    GoldToneMergeLayer=GoldToneMergeLayer,
)
