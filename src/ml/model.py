"""Model definitions and hyperparameter search spaces for Optuna."""

import optuna
from catboost import CatBoostClassifier
from lightgbm import LGBMClassifier
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier


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
        max_iter=5000,
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


def build_xgb(**params):
    """Build XGBoost classifier."""
    return XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        n_jobs=-1,
        random_state=42,
        **params,
    )


def build_lgbm(**params):
    """Build LightGBM classifier."""
    return LGBMClassifier(
        objective="binary",
        n_jobs=-1,
        random_state=42,
        **params,
    )


def build_catboost(**params):
    """Build CatBoost classifier."""
    return CatBoostClassifier(
        loss_function="Logloss",
        verbose=0,
        random_state=42,
        **params,
    )


# =========================
# PARAM SPACES (OPTUNA)
# =========================
def dummy_param_space(trial):
    """No hyperparameters for Dummy."""
    return {"strategy": "most_frequent"}


def logistic_param_space(trial):
    """Hyperparameter space for Logistic Regression.

    sklearn 1.8+: use ``l1_ratio`` (0 = L2, 1 = L1); do not pass deprecated ``penalty``.
    """
    l1_ratio = trial.suggest_categorical("l1_ratio", [0.0, 1.0])
    solver = trial.suggest_categorical("solver", ["liblinear", "saga", "lbfgs"])

    # lbfgs supports only L2
    if l1_ratio == 1.0 and solver == "lbfgs":
        raise optuna.exceptions.TrialPruned()

    return {
        "C": trial.suggest_float("C", 1e-4, 100.0, log=True),
        "l1_ratio": l1_ratio,
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


def xgb_param_space(trial):
    """Hyperparameter space for XGBoost."""
    return {
        "n_estimators": trial.suggest_int("n_estimators", 200, 800),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "gamma": trial.suggest_float("gamma", 0, 5),
        "reg_alpha": trial.suggest_float("reg_alpha", 0, 5),
        "reg_lambda": trial.suggest_float("reg_lambda", 0, 5),
    }


def lgbm_param_space(trial):
    """Hyperparameter space for LightGBM."""
    return {
        "n_estimators": trial.suggest_int("n_estimators", 200, 800),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "num_leaves": trial.suggest_int("num_leaves", 20, 150),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "reg_alpha": trial.suggest_float("reg_alpha", 0, 5),
        "reg_lambda": trial.suggest_float("reg_lambda", 0, 5),
    }


def catboost_param_space(trial):
    """Hyperparameter space for CatBoost."""
    return {
        "iterations": trial.suggest_int("iterations", 200, 800),
        "depth": trial.suggest_int("depth", 4, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
        "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1, 10),
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
        "xgboost": {
            "builder": build_xgb,
            "param_space": xgb_param_space,
        },
        "lightgbm": {
            "builder": build_lgbm,
            "param_space": lgbm_param_space,
        },
        "catboost": {
            "builder": build_catboost,
            "param_space": catboost_param_space,
        },
    }
