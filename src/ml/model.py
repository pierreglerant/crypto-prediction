"""Model definitions used in the benchmark."""

from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


def get_models():
    """Return a dictionary of initialized models.

    Returns:
        Dictionary mapping model names to model instances.
    """
    return {
        "dummy": DummyClassifier(strategy="most_frequent"),
        "logistic": LogisticRegression(
            class_weight="balanced",
            max_iter=1000,
        ),
        "random_forest": RandomForestClassifier(
            class_weight="balanced",
            random_state=42,
        ),
    }
