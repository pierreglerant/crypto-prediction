"""Public API for the ML benchmark package.

This package groups configuration, data preparation, model definitions,
training helpers, and evaluation metrics for time-series classification
benchmarks.
"""

from ml.config import N_SPLITS, N_TRIALS, RANDOM_STATE, TARGET, TEST_SIZE_RATIO
from ml.data import get_tscv, train_test_split_time
from ml.metrics import (
    calculate_metrics,
    plot_confusion_matrix,
)
from ml.model import get_models
from ml.training import benchmark_model, search_model_optuna

__all__ = [
    "TARGET",
    "TEST_SIZE_RATIO",
    "N_SPLITS",
    "N_TRIALS",
    "RANDOM_STATE",
    "train_test_split_time",
    "get_tscv",
    "calculate_metrics",
    "plot_confusion_matrix",
    "get_models",
    "benchmark_model",
    "search_model_optuna",
]
