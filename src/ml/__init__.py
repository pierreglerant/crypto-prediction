"""Public API for the ML benchmark package.

This package groups configuration, data preparation, model definitions,
training helpers, evaluation metrics, and the main pipeline entry point used
for time-series classification benchmarks.
"""

from ml.config import (
    N_SPLITS,
    PARAM_GRIDS,
    RANDOM_STATE,
    TARGET,
    TEST_SIZE_RATIO,
    THRESHOLD,
)
from ml.data import get_tscv, train_test_split_time
from ml.metrics import (
    calculate_means,
    calculate_metrics,
    plot_confusion_matrix,
)
from ml.model import get_models
from ml.pipeline import run_pipeline
from ml.training import benchmark_model, search_model

__all__ = [
    "TARGET",
    "TEST_SIZE_RATIO",
    "N_SPLITS",
    "THRESHOLD",
    "RANDOM_STATE",
    "PARAM_GRIDS",
    "train_test_split_time",
    "get_tscv",
    "calculate_metrics",
    "calculate_means",
    "plot_confusion_matrix",
    "get_models",
    "benchmark_model",
    "search_model",
    "run_pipeline",
]
