"""Model definitions and hyperparameter search spaces for Optuna."""

import optuna
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


# =========================
# BUILDERS
# =========================
def build_dummy(**params):
    """Build DummyClassifier."""
    return DummyClassifier(**params)


def build_logistic(**params):
    """Logistic Regression without preprocessing."""
    return LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        **params,
    )


def build_random_forest(**params):
    """Build RandomForestClassifier."""
    return RandomForestClassifier(
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
        **params,
    )


# =========================
# PARAM SPACES (OPTUNA)
# =========================
def dummy_param_space(trial):
    """No hyperparameters for Dummy."""
    return {"strategy": "most_frequent"}


def logistic_param_space(trial):
    """Hyperparameter space for Logistic Regression."""
    penalty = trial.suggest_categorical("penalty", ["l1", "l2"])
    solver = trial.suggest_categorical("solver", ["liblinear", "saga", "lbfgs"])

    # Remove invalid combinations
    if penalty == "l1" and solver == "lbfgs":
        raise optuna.exceptions.TrialPruned()

    return {
        # Inverse regularization strength (smaller = stronger regularization)
        "C": trial.suggest_float("C", 1e-4, 100.0, log=True),
        # Regularization type
        "penalty": penalty,
        # Optimization solver
        "solver": solver,
    }


def rf_param_space(trial):
    """Hyperparameter space for Random Forest."""
    return {
        "n_estimators": trial.suggest_int("n_estimators", 200, 800),
        "max_depth": trial.suggest_int("max_depth", 4, 12),
        "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 20),
        "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
        "max_features": trial.suggest_categorical("max_features", ["sqrt", "log2", 0.5]),
        "bootstrap": trial.suggest_categorical("bootstrap", [True, False]),
    }


# =========================
# MODEL REGISTRY
# =========================
def get_models():
    """Return model builders and their Optuna search spaces."""
    return {
        "dummy": {
            "builder": build_dummy,
            "param_space": dummy_param_space,
        },
        "logistic": {
            "builder": build_logistic,
            "param_space": logistic_param_space,
        },
        "random_forest": {
            "builder": build_random_forest,
            "param_space": rf_param_space,
        },
    }
