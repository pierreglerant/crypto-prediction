# GDELT Data Pipeline

Cryptocurrency media attention data pipeline using GDELT (Global Event Data, Location, and Tone) Project.

## Overview

Three-layer ETL pipeline for collecting and processing articles mentioning cryptocurrencies:

- **Bronze Layer**: Load from CSV source + optionally fetch missing dates from GDELT API
- **Silver Layer**: Clean, deduplicate, validate and normalize data
- **Gold Layer**: Aggregate by date and source domain

## Architecture

```
bitcoin_articles_bigquery.csv
          ↓
    [BRONZE LAYER]
          ↓
   bitcoin_bronze.jsonl
          ↓
    [SILVER LAYER]
          ↓
   bitcoin_silver.csv
          ↓
    [GOLD LAYER]
          ↓
   bitcoin_gold.csv  →  Ready for ML training
```

## Usage

### Quick Start

Run complete pipeline for Bitcoin:

```bash
python3 pipeline_runner.py
```

## Airflow Orchestration

The repository now includes an Airflow DAG for the GDELT pipeline:

- DAG file: `dags/gdelt_media_dag.py`
- DAG ID: `gdelt_media_dag`
- Schedule: daily at `02:00` (UTC)

### Start Airflow

```bash
docker-compose up airflow-init
docker-compose up -d airflow-scheduler airflow-webserver
```

Airflow UI is available at `http://localhost:8080`.

Default login:

- username: `admin`
- password: `admin`

### Runtime Variables

Configure variables from Airflow UI (`Admin -> Variables`):

- `GDELT_COIN` (default: `bitcoin`)
- `GDELT_QUERY_TERMS` (default: `bitcoin,btc`)
- `GDELT_FETCH_MISSING` (`true` or `false`, default: `false`)
- `GDELT_SOURCE_MAPPINGS_JSON` (optional JSON object)

Example for source mappings:

```json
{
      "cnn.com": "CNN",
      "reuters.com": "Reuters",
      "coindesk.com": "CoinDesk"
}
```

### Advanced Options

```bash
# Fetch missing recent dates from GDELT API
python3 pipeline_runner.py --fetch

# Process a different cryptocurrency
python3 pipeline_runner.py --coin ethereum --terms ethereum eth

# Custom search terms
python3 pipeline_runner.py --terms bitcoin btc "bitcoin cash"
```

### Individual Layer Execution

Run specific layers:

```bash
# Bronze layer only
python3 bronze_layer.py
python3 bronze_layer.py --fetch  # with GDELT fetch

# Silver layer
python3 silver_layer.py

# Gold layer
python3 gold_layer.py
```

## Input Format

### Local-first article cache

Expected columns:
- `day` or `date` (YYYY-MM-DD or YYYYMMDD format)
- `url` or `URL` or `DocumentIdentifier`

Example:
```csv
day,url
2015-01-01,https://example.com/article1
2015-01-02,https://example.com/article2
```

File naming convention:
- `{coin_name}_articles_bigquery.csv`

Example: `bitcoin_articles_bigquery.csv`

### Local-first tone summary

The tone path is now **cache-first** and uses `data/cache/{coin_name}_tone_count_1d.csv` by default.
If the cache is missing, the layer can fall back to BigQuery, fetch the daily tone/count aggregation,
and persist it back to the same cache CSV format for reuse.

Expected columns:
- `date` or `day`
- `avg_tone`
- `article_count`

The future BigQuery query is prepared with placeholders in `src/config/gcp.py` and can be enabled later with credentials such as:
- `GOOGLE_CLOUD_PROJECT`
- `BIGQUERY_DATASET`
- `BIGQUERY_LOCATION`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `GCP_SERVICE_ACCOUNT_JSON`

## Output Format

### Final Output (Gold CSV)

Contains daily article counts by source domain:

```csv
date,CNN,Reuters,CoinDesk,Other,total
2015-01-01,5,3,2,15,25
2015-01-02,4,2,3,12,21
```

File naming convention:
- `{coin_name}_gold.csv`

## Configuration

### Source Mappings (Gold Layer)

Customize domain → source mappings by modifying `source_mappings` parameter:

```python
from gold_layer import GoldLayer

mappings = {
    "cnn.com": "CNN",
    "reuters.com": "Reuters",
    "mysite.com": "My Site"
}

gold = GoldLayer("bitcoin", source_mappings=mappings)
gold.run()
```

## Cache Files

All intermediate and cache files are in `.gitignore`:
- `*_bronze.jsonl` - Raw enriched articles
- `*_silver.csv` - Cleaned deduplicated data
- `*_bigquery.csv` - Legacy BigQuery exports / fallback inputs
- `*.json` - State files

## GDELT API Integration

Automatic fetch of missing dates using GDELT DOC 2.0 API:

- Query format: `(term1 OR term2 OR term3)`
- Time window: Last 3 months maximum (GDELT API limitation)
- Rate limiting: 5 seconds between requests
- Adaptive chunking: Automatically adjusts request window based on article volume

## Performance

Expected performance with ~4M articles:

- Bronze: 1-2 minutes (CSV loading)
- Silver: 10-15 minutes (deduplication)
- Gold: 5-10 minutes (aggregation)

**Total: ~20-30 minutes for full pipeline**

## Extensibility

Add new cryptocurrencies easily:

```python
from pipeline_runner import run_pipeline

# Bitcoin
run_pipeline("bitcoin", ["bitcoin", "btc"])

# Ethereum
run_pipeline("ethereum", ["ethereum", "eth"])

# Dogecoin
run_pipeline("dogecoin", ["dogecoin", "doge"])
```

Each cryptocurrency maintains separate cache files.

## Files Overview

| File | Purpose |
|------|---------|
| `bronze_layer.py` | Load CSV, optionally fetch from GDELT |
| `silver_layer.py` | Clean, deduplicate, validate |
| `gold_layer.py` | Aggregate by date and source |
| `pipeline_runner.py` | Orchestrate all layers |
| `dags/gdelt_media_dag.py` | Airflow DAG orchestration |

## Error Handling

- **Rate limiting**: Automatic 60-second backoff on HTTP 429
- **Invalid data**: Rows with missing required fields are skipped
- **Duplicates**: URL-based deduplication in Silver layer
- **Resumable**: Each layer is independent; can re-run individual layers

## Notes

- GDELT API historical limit: 3 months of rolling window
- BigQuery export provides historical data (back to ~2015)
- CSV cache files should be in `.gitignore` (not tracked in Git)
- The tone summary is generated locally from the existing gold tone output; no manual `tone_count_1d.csv` export is required for the current mode
- Pipeline is fully agnostic to cryptocurrency type
