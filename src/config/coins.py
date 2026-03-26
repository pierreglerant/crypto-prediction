"""Canonical coin configuration for Airflow DAG generation.

This registry centralizes the per-coin defaults used by market and GDELT
pipelines so new cryptocurrencies can be added without duplicating DAG logic.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketCoinConfig:
    """Configuration for the Binance market data DAG."""

    coin_name: str
    market_symbol: str
    artifact_symbol: str
    variable_prefix: str
    dag_id: str
    description: str
    tags: tuple[str, ...]
    default_interval: str = "1d"
    default_start_date: str = "2017-01-01"


@dataclass(frozen=True)
class GdeltCoinConfig:
    """Configuration for the GDELT media DAG."""

    coin_name: str
    variable_prefix: str
    dag_id: str
    description: str
    tags: tuple[str, ...]
    default_query_terms: tuple[str, ...]
    source_mappings_var: str
    fetch_missing_var: str
    tone_enabled: bool = False


BTC_MARKET = MarketCoinConfig(
    coin_name="bitcoin",
    market_symbol="BTCUSDT",
    artifact_symbol="btc_usdt",
    variable_prefix="BTC",
    dag_id="btc_market_pipeline",
    description="Daily BTC/USDT market data pipeline: Binance ingestion to gold features",
    tags=("btc", "market", "binance", "etl"),
)

ETH_MARKET = MarketCoinConfig(
    coin_name="ethereum",
    market_symbol="ETHUSDT",
    artifact_symbol="eth_usdt",
    variable_prefix="ETH",
    dag_id="eth_market_pipeline",
    description="Daily ETH/USDT market data pipeline: Binance ingestion to gold features",
    tags=("eth", "market", "binance", "etl"),
)

XRP_MARKET = MarketCoinConfig(
    coin_name="xrp",
    market_symbol="XRPUSDT",
    artifact_symbol="xrp_usdt",
    variable_prefix="XRP",
    dag_id="xrp_market_pipeline",
    description="Daily XRP/USDT market data pipeline: Binance ingestion to gold features",
    tags=("xrp", "market", "binance", "etl"),
)

LTC_MARKET = MarketCoinConfig(
    coin_name="litecoin",
    market_symbol="LTCUSDT",
    artifact_symbol="ltc_usdt",
    variable_prefix="LTC",
    dag_id="ltc_market_pipeline",
    description="Daily LTC/USDT market data pipeline: Binance ingestion to gold features",
    tags=("ltc", "market", "binance", "etl"),
)

BCH_MARKET = MarketCoinConfig(
    coin_name="bitcoin_cash",
    market_symbol="BCHUSDT",
    artifact_symbol="bch_usdt",
    variable_prefix="BCH",
    dag_id="bch_market_pipeline",
    description="Daily BCH/USDT market data pipeline: Binance ingestion to gold features",
    tags=("bch", "market", "binance", "etl"),
)

ADA_MARKET = MarketCoinConfig(
    coin_name="cardano",
    market_symbol="ADAUSDT",
    artifact_symbol="ada_usdt",
    variable_prefix="ADA",
    dag_id="ada_market_pipeline",
    description="Daily ADA/USDT market data pipeline: Binance ingestion to gold features",
    tags=("ada", "market", "binance", "etl"),
)

DOGE_MARKET = MarketCoinConfig(
    coin_name="dogecoin",
    market_symbol="DOGEUSDT",
    artifact_symbol="doge_usdt",
    variable_prefix="DOGE",
    dag_id="doge_market_pipeline",
    description="Daily DOGE/USDT market data pipeline: Binance ingestion to gold features",
    tags=("doge", "market", "binance", "etl"),
)

BTC_GDELT = GdeltCoinConfig(
    coin_name="bitcoin",
    variable_prefix="GDELT",
    dag_id="gdelt_media_dag",
    description="Daily robust GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "etl"),
    default_query_terms=("bitcoin", "btc"),
    source_mappings_var="GDELT_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_FETCH_MISSING",
    tone_enabled=True,
)

ETH_GDELT = GdeltCoinConfig(
    coin_name="ethereum",
    variable_prefix="GDELT_ETH",
    dag_id="gdelt_media_eth_dag",
    description="Daily Ethereum GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "eth", "etl"),
    default_query_terms=("ethereum", "eth"),
    source_mappings_var="GDELT_ETH_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_ETH_FETCH_MISSING",
    tone_enabled=True,
)

XRP_GDELT = GdeltCoinConfig(
    coin_name="xrp",
    variable_prefix="GDELT_XRP",
    dag_id="gdelt_media_xrp_dag",
    description="Daily XRP GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "xrp", "etl"),
    default_query_terms=("xrp", "ripple"),
    source_mappings_var="GDELT_XRP_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_XRP_FETCH_MISSING",
    tone_enabled=True,
)

LTC_GDELT = GdeltCoinConfig(
    coin_name="litecoin",
    variable_prefix="GDELT_LTC",
    dag_id="gdelt_media_ltc_dag",
    description="Daily Litecoin GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "ltc", "etl"),
    default_query_terms=("litecoin", "ltc"),
    source_mappings_var="GDELT_LTC_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_LTC_FETCH_MISSING",
    tone_enabled=True,
)

BCH_GDELT = GdeltCoinConfig(
    coin_name="bitcoin_cash",
    variable_prefix="GDELT_BCH",
    dag_id="gdelt_media_bch_dag",
    description="Daily Bitcoin Cash GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "bch", "etl"),
    default_query_terms=("bitcoin cash", "bch"),
    source_mappings_var="GDELT_BCH_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_BCH_FETCH_MISSING",
    tone_enabled=True,
)

ADA_GDELT = GdeltCoinConfig(
    coin_name="cardano",
    variable_prefix="GDELT_ADA",
    dag_id="gdelt_media_ada_dag",
    description="Daily Cardano GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "ada", "etl"),
    default_query_terms=("cardano", "ada"),
    source_mappings_var="GDELT_ADA_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_ADA_FETCH_MISSING",
    tone_enabled=True,
)

DOGE_GDELT = GdeltCoinConfig(
    coin_name="dogecoin",
    variable_prefix="GDELT_DOGE",
    dag_id="gdelt_media_doge_dag",
    description="Daily Dogecoin GDELT ingestion and media aggregation pipeline",
    tags=("gdelt", "media", "doge", "etl"),
    default_query_terms=("dogecoin", "doge"),
    source_mappings_var="GDELT_DOGE_SOURCE_MAPPINGS_JSON",
    fetch_missing_var="GDELT_DOGE_FETCH_MISSING",
    tone_enabled=True,
)
