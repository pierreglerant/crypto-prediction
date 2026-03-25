"""Global configuration for the ML benchmark pipeline.

TARGET: The name of the target variable in the dataset.
TEST_SIZE_RATIO: The proportion of the dataset to include in the test split.
N_SPLITS: The number of folds for cross-validation.
THRESHOLD: The threshold for classifying probabilities into binary classes.
RANDOM_STATE: The random seed for reproducibility.
PARAM_GRIDS: A dictionary containing hyperparameter grids for different models to be used in grid search
"""

TARGET = "target"

TEST_SIZE_RATIO = 0.2

N_SPLITS = 5
THRESHOLD = 0.2

RANDOM_STATE = 42

PARAM_GRIDS = {
    "random_forest": {
        "n_estimators": [100, 200],
        "max_depth": [3, 5, 8],
        "min_samples_leaf": [1, 5, 10],
    },
    "logistic": {
        "C": [0.1, 1, 10],
    },
}
