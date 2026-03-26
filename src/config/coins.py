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
    tone_input_var: str | None = None


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
    tone_input_var="GDELT_TONE_INPUT_CSV",
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
    tone_enabled=False,
)
