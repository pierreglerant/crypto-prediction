"""Batch processing, ETL steps, and orchestrated data workflows."""

__all__ = ["run_benchmark_pipeline"]


def __getattr__(name: str):
    """Lazily expose optional pipeline entry points."""
    if name == "run_benchmark_pipeline":
        from pipelines.ml_benchmark import run_benchmark_pipeline

        return run_benchmark_pipeline

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
