"""Data utilities for time-based splitting and cross-validation."""

from sklearn.model_selection import TimeSeriesSplit


def train_test_split_time(X, y, test_size=0.2):
    """Split dataset into train and test sets using chronological order.

    Args:
        X: Feature matrix.
        y: Target vector.
        test_size: Fraction of data used for testing.

    Returns:
        Tuple of (X_train, X_test, y_train, y_test).
    """
    split_idx = int(len(X) * (1 - test_size))
    return (
        X.iloc[:split_idx],
        X.iloc[split_idx:],
        y.iloc[:split_idx],
        y.iloc[split_idx:],
    )


def get_tscv(n_splits=5):
    """Create a TimeSeriesSplit object.

    Args:
        n_splits: Number of folds.

    Returns:
        Configured TimeSeriesSplit instance.
    """
    return TimeSeriesSplit(n_splits=n_splits)
