"""Lightweight namespace for ML utilities.

Keep this module import-safe for Airflow DAG parsing: do not import sklearn or
other optional ML dependencies at module import time.
"""

from .config import N_SPLITS, N_TRIALS, RANDOM_STATE, TARGET, TEST_SIZE_RATIO

__all__ = ["TARGET", "TEST_SIZE_RATIO", "N_SPLITS", "N_TRIALS", "RANDOM_STATE"]
