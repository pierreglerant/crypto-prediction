# 🚀 Crypto Data Platform

A production-grade **Data Engineering project** that ingests, processes, and serves cryptocurrency market data.
This project demonstrates how to build a **modular, scalable data platform** from raw API data to analytics-ready datasets and APIs.

---

# 🧭 Overview

The platform:

* Collects crypto data from external APIs (CoinGecko, Binance)
* Stores raw and processed data in a structured warehouse
* Applies transformations (cleaning, feature engineering)
* Exposes data via a REST API
* (Optional) Supports ML and real-time streaming pipelines

---

# 🏗️ Project Architecture

```
External APIs → Ingestion → Raw Storage → Transformations → Data Warehouse → API → Dashboard / ML
```

---

# 📂 Project Structure

## 📦 `src/` — Core application code

Python packages at the root of `src/` (`ingestion`, `pipelines`, `api`, etc.) contain the reusable business logic.

### 🔹 `ingestion/`

Handles data collection from external APIs.

* API clients (CoinGecko, Binance)
* Request handling, retries, parsing
* Normalization of raw data

👉 Example:

```python
coingecko_client.py
binance_client.py
```

---

### 🔹 `pipelines/`

Contains the **ETL logic**.

* Data cleaning pipelines
* Feature engineering
* Validation logic

👉 Responsibilities:

* Transform raw → clean → analytics-ready data
* Ensure consistency and quality

---

### 🔹 `api/`

FastAPI service exposing the data.

* REST endpoints (`/prices`, `/metrics`)
* Database queries
* Business logic layer

👉 Turns the data platform into a **data product**

---

### 🔹 `ml/` (optional)

Machine Learning components.

* Dataset preparation
* Feature engineering
* Model training & inference

👉 Enables predictive analytics or trading strategies

---

### 🔹 `streaming/` (optional)

Real-time data processing.

* Kafka producers/consumers
* Streaming pipelines

👉 Used for near real-time analytics

---

### 🔹 `config/`

Centralized configuration.

* API URLs
* Database settings
* Environment variables

👉 Avoids hardcoding values across the project

---

### 🔹 `utils/`

Shared utilities.

* Logging
* Helpers
* Common functions

---

# ⏱️ `dags/` — Airflow orchestration

Defines workflow scheduling and dependencies.

* DAGs for ingestion and transformation
* Task orchestration (`fetch → store → transform`)

👉 This is where pipelines are automated

---

# 🧱 `dbt/` — Data transformations

SQL-based transformations using dbt.

* `staging/` → raw data cleaning
* `intermediate/` → transformations
* `marts/` → analytics tables

👉 Implements the **Bronze / Silver / Gold** pattern

---

# 🗄️ `warehouse/` — Database layer

Manages the data warehouse schema.

* Table definitions (raw, clean, analytics)
* Indexes and partitioning
* Migrations

👉 Ensures scalable and optimized storage

---

# 🧪 `tests/` — Testing suite

Contains:

* Unit tests (Python)
* Data tests (SQL/dbt)

👉 Guarantees correctness and reliability

---

# 🐳 `docker/` — Containerization

Docker configuration for all services.

* API service
* Database
* Airflow

👉 Enables reproducible environments

---

# ⚙️ `scripts/` — CLI utilities

Helper scripts for development and operations.

* Manual ingestion
* Backfills
* Debug tools

---

# 📚 `docs/` — Documentation

Project documentation and design decisions.

* Architecture diagrams
* Data model description
* Technical decisions

---

# 📓 `notebooks/` — Exploration

Jupyter notebooks for:

* Data exploration
* Prototyping features
* ML experiments

👉 Not used in production

---

# 📦 Root files

### 🔹 `pyproject.toml`

Defines:

* dependencies
* packaging
* tooling (lint, format)

---

### 🔹 `docker-compose.yml`

Runs the full stack locally:

```bash
docker-compose up
```

Services:

* PostgreSQL
* Airflow
* API

---

### 🔹 `README.md`

Project documentation (this file)

---

# ⚡ How to Run

```bash
# start all services
docker-compose up --build
```

Then:

* API → http://localhost:8000
* Airflow → http://localhost:8080

---

# 📊 Data Flow

1. Fetch crypto data from API
2. Store raw data in database
3. Transform data (dbt / pipelines)
4. Store analytics-ready tables
5. Serve via API

---

# 🧠 Key Concepts Demonstrated

* Data ingestion from external APIs
* ETL pipeline design
* Data warehouse modeling
* Orchestration with Airflow
* API-based data exposure
* Modular architecture (src-based)

---

# 🔥 Future Improvements

* Add streaming pipeline (Kafka)
* Integrate feature store
* Add ML predictions
* Implement monitoring & alerting
* Deploy on cloud (AWS / GCP)

---

# 🎯 Goal of the Project

This project demonstrates how to move from:

> raw API data → production-ready data platform

It is designed to showcase **real-world Data Engineering skills**:

* scalability
* modularity
* reliability
* production readiness

---
