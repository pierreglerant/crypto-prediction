"""FastAPI entrypoint for the crypto data platform HTTP API."""

import os

from fastapi import FastAPI

app = FastAPI(
    title="Crypto Data Platform API",
    description="REST API over warehouse and analytics data.",
    version="0.1.0",
)


@app.get("/", tags=["health"])
def read_root() -> dict[str, str]:
    """Return a short welcome payload."""
    return {"service": "crypto-platform", "status": "ok"}


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    """Liveness probe for orchestrators and load balancers."""
    return {"status": "healthy"}


@app.get("/config/db", tags=["health"])
def db_config() -> dict[str, str | None]:
    """Expose whether a database URL is configured (value redacted)."""
    url = os.environ.get("DATABASE_URL")
    return {"database_configured": "yes" if url else "no", "database_url": None}
