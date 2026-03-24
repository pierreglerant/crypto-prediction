#!/usr/bin/env python3
"""
Configuration examples for multi-cryptocurrency pipeline.

Shows how to process different cryptocurrencies
"""

# Example 1: Bitcoin (default)
BITCOIN_CONFIG = {
    'coin_name': 'bitcoin',
    'query_terms': ['bitcoin', 'btc'],
    'source_mappings': {
        'cnn.com': 'CNN',
        'reuters.com': 'Reuters',
        'bloomberg.com': 'Bloomberg',
        'coindesk.com': 'CoinDesk',
        'cointelegraph.com': 'Cointelegraph',
    }
}

# Example 2: Ethereum
ETHEREUM_CONFIG = {
    'coin_name': 'ethereum',
    'query_terms': ['ethereum', 'eth'],
    'source_mappings': {
        'cnn.com': 'CNN',
        'reuters.com': 'Reuters',
        'bloomberg.com': 'Bloomberg',
        'coindesk.com': 'CoinDesk',
        'cointelegraph.com': 'Cointelegraph',
    }
}

# Example 3: Dogecoin
DOGECOIN_CONFIG = {
    'coin_name': 'dogecoin',
    'query_terms': ['dogecoin', 'doge'],
    'source_mappings': {
        'cnn.com': 'CNN',
        'coindesk.com': 'CoinDesk',
        'cointelegraph.com': 'Cointelegraph',
    }
}

# Example usage:
if __name__ == "__main__":
    from pipeline_runner import run_pipeline

    # Process Bitcoin with GDELT fetch
    print("Processing Bitcoin...")
    run_pipeline(**BITCOIN_CONFIG, fetch_missing=True)

    # Process Ethereum
    print("\n\nProcessing Ethereum...")
    run_pipeline(**ETHEREUM_CONFIG, fetch_missing=False)

    # Process Dogecoin
    print("\n\nProcessing Dogecoin...")
    run_pipeline(**DOGECOIN_CONFIG, fetch_missing=False)
