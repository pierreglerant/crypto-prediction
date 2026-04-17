"""Google Cloud configuration helpers and BigQuery query templates.

This module centralizes the BigQuery settings and SQL builders used by the
GDELT pipeline so the DAGs can run without any GDELT API calls.
"""

from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from pathlib import Path


def _escape_sql_literal(value: str) -> str:
    """Escape a string for safe embedding in a SQL LIKE literal."""
    return value.replace("'", "''")


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


def build_daily_article_cache_query(query_terms: list[str] | tuple[str, ...], start_date: str | None = None, end_date: str | None = None) -> str:
    """Build the BigQuery SQL for a daily article cache.

    The query extracts article URLs and publication days from the GKG table so
    the bronze layer can materialize the local article cache without using the
    GDELT DOC API.
    """
    cleaned_terms = [term.strip().lower() for term in query_terms if term and term.strip()]
    if not cleaned_terms:
        raise ValueError("At least one query term is required")

    term_predicates = [f"LOWER(COALESCE(V2Themes, '')) LIKE '%{_escape_sql_literal(term)}%'" for term in cleaned_terms]

    date_clause = ""
    if start_date and end_date:
        start_key = start_date.replace("-", "")
        end_key = end_date.replace("-", "")
        date_clause = f"  AND DATE BETWEEN {start_key}000000 AND {end_key}235959\n"

    return f"""
SELECT
  SUBSTR(CAST(DATE AS STRING), 1, 8) AS day,
  DocumentIdentifier AS url
FROM
  `gdelt-bq.gdeltv2.gkg`
WHERE
  DocumentIdentifier IS NOT NULL
  AND ({" OR ".join(term_predicates)})
{date_clause}ORDER BY
  day,
  url;
""".strip()
