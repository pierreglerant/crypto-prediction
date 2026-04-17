# crypto-prediction

End-to-end pipeline for cryptocurrency crash-regime detection, combining market data, media signals, and cross-asset features. Built as an empirical study to assess whether external data sources improve prediction beyond a market-only baseline.

---

## What this project does

1. **Ingests** daily OHLCV data from Binance and media data from GDELT
2. **Processes** raw data through a Bronze → Silver → Gold pipeline
3. **Engineers features** (returns, volatility, drawdown, buy pressure, media signals, cross-asset indicators)
4. **Benchmarks** six model families (Logistic Regression, Random Forest, XGBoost, LightGBM, CatBoost, Dummy) with Bayesian hyperparameter optimization (Optuna)
5. **Evaluates** under strict chronological train/test splits using PR-AUC as the primary metric

The key finding: for BTC, a CatBoost model on 17 market features achieves PR-AUC = 0.771. Adding media or cross-asset features consistently degrades performance for both BTC and ETH.

---

## Project structure

```
src/
├── ingestion/        # Binance and GDELT API clients
├── processing/       # Bronze → Silver → Gold pipelines (BTC, ETH, media)
├── pipelines/        # ML benchmark pipeline
├── ml/               # Models, training, metrics, config
└── config/           # Global settings

notebooks/
├── eda/              # Exploratory analysis (01–07)
└── benchmark/        # Model benchmarks (01–08)

docs/
├── paper_crypto_crash.tex        # Research paper (LaTeX)
└── etude_predictibilite_crypto.md  # Study report (French)

dags/                 # Airflow DAGs for pipeline orchestration
dbt/                  # dbt transformations
tests/                # Unit and integration tests
```

---

## Notebooks

### EDA
| Notebook | Content |
|----------|---------|
| `01_eda_btc` | BTC market feature analysis and selection |
| `02_eda_media` | GDELT media pipeline QC and signal analysis |
| `03_eda_eth` | ETH market feature analysis and selection |
| `04_eda_btc_price_and_media` | BTC market + media feature scoring |
| `05_eda_eth_price_and_media` | ETH market + media feature scoring |
| `06_eda_btc_with_eth` | ETH cross-asset features for BTC target |
| `07_eda_eth_with_btc` | BTC cross-asset features for ETH target |

### Benchmarks
| Notebook | Target | Features | Best PR-AUC |
|----------|--------|----------|-------------|
| `01_model_benchmark_btc` | BTC | 17 market | **0.771** |
| `02_model_benchmark_eth` | ETH | 17 market | **0.180** |
| `03_model_benchmark_btc_media` | BTC | 17 + 7 media | 0.760 |
| `04_model_benchmark_eth_media` | ETH | 17 + 3 media | 0.169 |
| `05_model_benchmark_btc_with_eth` | BTC | 17 + 17 ETH cross | 0.766 |
| `05b_model_benchmark_btc_with_eth_top5` | BTC | 17 + 5 ETH cross | 0.766 |
| `06_model_benchmark_eth_with_btc` | ETH | 17 + 14 BTC cross | 0.179 |
| `07_model_benchmark_eth_btc_media` | ETH | 17 + 14 BTC + 3 media | 0.152 |
| `08_model_benchmark_btc_eth_media` | BTC | 17 + 17 ETH + 7 media | 0.755 |

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Run tests:
```bash
pytest tests/
```

Run the full benchmark pipeline:
```bash
# Open notebooks/benchmark/ in Jupyter and run in order
jupyter lab
```

---

## ML configuration

Key parameters in `src/ml/config.py`:

| Parameter | Value |
|-----------|-------|
| Train/test split | 80 / 20 (chronological) |
| CV folds | 5 (TimeSeriesSplit) |
| Optuna trials | 100 |
| Primary metric | PR-AUC |
| Random state | 42 |

---

## Data

- **Market data**: Binance public API (BTCUSDT, ETHUSDT), daily OHLCV, 2017-09-16 → 2026-03-27
- **Media data**: GDELT Project, Bitcoin-related articles, daily aggregation, 2015 → 2026
- Data is stored under `data/` following the Bronze / Silver / Gold layering

---

## Report

The full research paper is available at [`report.pdf`](report.pdf).

---

## Authors

Pierre Glerant · Hamza Errahj — CentraleSupélec, Université Paris-Saclay
