"""Google Cloud configuration helpers and BigQuery query templates.

This module is intentionally local-first for now: it stores placeholders for
future BigQuery integration, but it does not require cloud credentials to run
the current pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from pathlib import Path


@dataclass(frozen=True)
class BigQuerySettings:
    """Placeholder settings for a future BigQuery-backed ingestion mode."""

    enabled: bool
    project_id: str | None
    dataset: str | None
    location: str
    service_account_json: str | None
    credentials_path: Path | None


def load_bigquery_settings(enabled: bool = False) -> BigQuerySettings:
    """Load BigQuery placeholders from environment variables.

    The variables are intentionally generic so they can be wired either from
    local WSL development, Docker Compose, or Airflow Variables later.
    """
    credentials_path_raw = getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip() or None
    return BigQuerySettings(
        enabled=enabled,
        project_id=getenv("GOOGLE_CLOUD_PROJECT", "").strip() or None,
        dataset=getenv("BIGQUERY_DATASET", "").strip() or None,
        location=getenv("BIGQUERY_LOCATION", "EU").strip() or "EU",
        service_account_json=getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip() or None,
        credentials_path=Path(credentials_path_raw) if credentials_path_raw else None,
    )


def build_daily_tone_count_query(coin_name: str) -> str:
    """Build the future BigQuery SQL for daily tone/count aggregation.

    The query is parameterized by coin name so it can be reused for Bitcoin,
    Ethereum, or any other supported asset.
    """
    coin_term = coin_name.strip().lower()
    return f"""
SELECT
  DATE(PARSE_DATE('%Y%m%d', CAST(DATE AS STRING))) AS jour,
  COUNT(*) AS nb_articles,
  AVG(SAFE_CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) AS avg_tone
FROM
  `gdelt-bq.gdeltv2.gkg`
WHERE
  LOWER(V2Themes) LIKE '%{coin_term}%'
GROUP BY
  jour
ORDER BY
  jour;
""".strip()
