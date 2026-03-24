#!/usr/bin/env python3
"""
PIPELINE: Orchestrate bronze, silver, and gold layers.

Agnostic cryptocurrency data pipeline
"""
import argparse

from bronze_layer import BronzeLayer
from gold_layer import GoldLayer
from silver_layer import SilverLayer


def run_pipeline(coin_name, query_terms, fetch_missing=False, source_mappings=None):
    """
    Execute complete pipeline: bronze → silver → gold.

    Args:
        coin_name: Cryptocurrency name (bitcoin, ethereum, etc.)
        query_terms: List of search terms for GDELT queries
        fetch_missing: Whether to fetch missing dates from GDELT
        source_mappings: Optional custom source mappings
    """
    print(f"\n{'='*60}")
    print(f"🔗 PIPELINE: {coin_name.upper()} DATA PROCESSING")
    print(f"{'='*60}\n")

    # Bronze layer
    print(f"\n{'='*60}")
    print("📦 STAGE 1: BRONZE LAYER")
    print(f"{'='*60}\n")
    bronze = BronzeLayer(coin_name, query_terms)
    bronze.run(fetch_missing=fetch_missing)

    # Silver layer
    print(f"\n{'='*60}")
    print("🧹 STAGE 2: SILVER LAYER")
    print(f"{'='*60}\n")
    silver = SilverLayer(coin_name)
    silver.run()

    # Gold layer
    print(f"\n{'='*60}")
    print("🏆 STAGE 3: GOLD LAYER")
    print(f"{'='*60}\n")
    gold = GoldLayer(coin_name, source_mappings=source_mappings)
    gold.run()

    print(f"\n{'='*60}")
    print("✅ PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"\nOutput data: {coin_name}_gold.csv")
    print("Ready for ML model training!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cryptocurrency article data pipeline"
    )

    parser.add_argument(
        '--coin',
        type=str,
        default='bitcoin',
        help='Cryptocurrency name (default: bitcoin)'
    )

    parser.add_argument(
        '--terms',
        type=str,
        nargs='+',
        default=['bitcoin', 'btc'],
        help='Search terms for GDELT API'
    )

    parser.add_argument(
        '--fetch',
        action='store_true',
        help='Fetch missing dates from GDELT API'
    )

    args = parser.parse_args()

    run_pipeline(
        coin_name=args.coin,
        query_terms=args.terms,
        fetch_missing=args.fetch
    )
