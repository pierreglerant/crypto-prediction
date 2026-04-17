"""Global configuration for the ML benchmark pipeline.

TARGET: Name of the target column in the dataset.
TEST_SIZE_RATIO: Proportion of data reserved for testing.
N_SPLITS: Number of folds for time-series cross-validation.
THRESHOLD: Decision threshold for binary classification.
RANDOM_STATE: Seed for reproducibility.
N_TRIALS: Number of trials for Optuna hyperparameter optimization.
"""

TARGET = "target"

TEST_SIZE_RATIO = 0.2

N_SPLITS = 5

RANDOM_STATE = 42

# Optuna
N_TRIALS = 100
