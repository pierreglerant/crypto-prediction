# Production-Ready Pipeline Structure

## Clean Architecture Overview

```
src/ingestion/GDELT/
├── bronze_layer.py           # Load CSV + optional GDELT fetch
├── silver_layer.py           # Clean, deduplicate, validate
├── gold_layer.py             # Aggregate by date/source
├── gdelt_client.py           # GDELT API client
├── pipeline_runner.py        # Orchestrate all layers
├── config_examples.py        # Configuration examples for multiple coins
├── README.md                 # User documentation
├── .gitignore               # Cache file exclusions
│
└── [Cache Files - Not Tracked]
    ├── bitcoin_articles_bigquery.csv    (444MB - ignored)
    ├── bitcoin_bronze.jsonl              (ignored)
    ├── bitcoin_silver.csv                (ignored)
    ├── bitcoin_gold.csv                  (ignored)
    └── ethereum_*.csv, dogecoin_*.csv   (ignored)
```

## Key Improvements

### 1. Single Responsibility Principle
- **BronzeLayer**: Only handles loading and GDELT fetching
- **SilverLayer**: Only handles cleaning and deduplication
- **GoldLayer**: Only handles aggregation
- **GDELTClient**: Only handles API communication

### 2. Cryptocurrency Agnostic Design
```python
# Same code works for any cryptocurrency
BronzeLayer("bitcoin", ["bitcoin", "btc"])
BronzeLayer("ethereum", ["ethereum", "eth"])
BronzeLayer("dogecoin", ["dogecoin", "doge"])
```

### 3. Clean Naming Conventions
- Production files follow pattern: `{functionality}_layer.py`
- Cache files follow pattern: `{coin}_{layer}.{ext}`
- All cache files in `.gitignore`

### 4. Parameterized Configuration
```python
# Set custom source mappings per cryptocurrency
gold = GoldLayer("bitcoin", source_mappings={
    "cnn.com": "CNN",
    "my_site.com": "My Site"
})
```

### 5. Simplified CLI Interface
```bash
# Bitcoin default
python3 pipeline_runner.py

# Custom cryptocurrency
python3 pipeline_runner.py --coin ethereum --terms ethereum eth

# With GDELT fetch
python3 pipeline_runner.py --fetch
```

### 6. Full Extensibility
- Add new cryptocurrencies without code changes
- Customize source mappings per coin
- Reuse layers independently
- Swap implementations without affecting others

## Git Workflow

```bash
# Before pushing
cd src/ingestion/GDELT

# Verify structure is clean
ls -la *.py    # Only 5 production files
ls -la *.csv   # These are IN .gitignore

# Check git status
git status     # Cache files should NOT appear

# Push clean code
git add .
git commit -m "Refactor: Production-ready multi-cryptocurrency pipeline"
git push
```

## Production Checklist

✅ All old scripts removed (article_counter.py, bronze_fetch_articles.py, etc.)
✅ Cache files properly excluded (.gitignore)
✅ 4 core modules implemented (bronze, silver, gold, gdelt_client)
✅ Cryptocurrency agnostic design
✅ CLI interface with argument parsing
✅ Comprehensive documentation (README.md)
✅ Configuration examples for multiple coins
✅ All modules import successfully
✅ Ready for multi-cryptocurrency scaling

## Performance Notes

With 4M articles from BigQuery:
- Bronze: 1-2 min (CSV load)
- Silver: 10-15 min (dedup)
- Gold: 5-10 min (agg)
- **Total: ~20-30 min**

Scales linearly with article count.
