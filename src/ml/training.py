"""Training and evaluation utilities with Optuna-based hyperparameter search."""

import numpy as np
import optuna
from sklearn.metrics import average_precision_score, f1_score

from ml.metrics import (
    calculate_metrics,
    plot_confusion_matrix,
)


def find_best_threshold(y_true, y_proba):
    """Find optimal threshold maximizing F1 score."""
    thresholds = np.linspace(0.01, 0.5, 100)

    best_t = 0.2
    best_score = -1

    for t in thresholds:
        y_pred = (y_proba > t).astype(int)
        score = f1_score(y_true, y_pred, zero_division=0)

        if score > best_score:
            best_score = score
            best_t = t

    return best_t


def benchmark_model(
    model,
    X,
    y,
    tscv,
    threshold=None,
    verbose=True,
    plot_confusion=True,
):
    """Evaluate a model using time-series cross-validation.

    If threshold is None, it is optimized globally using OOF predictions.

    Args:
        model: Model instance.
        X: Feature matrix.
        y: Target vector.
        tscv: TimeSeriesSplit object.
        threshold: Decision threshold (if None → optimized).
        verbose: Whether to print metrics.
        plot_confusion: Whether to display confusion matrix.

    Returns:
        Dictionary of evaluation metrics computed on global OOF predictions.
    """
    y_true_all = []
    y_proba_all = []

    # Step 1 — Collect OOF probabilities
    for train_idx, val_idx in tscv.split(X):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model.fit(X_tr, y_tr)
        y_proba = model.predict_proba(X_val)[:, 1]

        y_true_all.extend(y_val)
        y_proba_all.extend(y_proba)

    y_true_all = np.array(y_true_all)
    y_proba_all = np.array(y_proba_all)

    # Step 2 — Find best threshold if not provided
    if threshold is None:
        threshold = find_best_threshold(y_true_all, y_proba_all)

    if verbose:
        print(f"\n=== Best Threshold === {threshold:.4f}")

    # Step 3 — Apply threshold and compute metrics
    y_pred_all = (y_proba_all > threshold).astype(int)
    metrics = calculate_metrics(y_true_all, y_pred_all, y_proba_all)

    if verbose:
        print("\n=== Metrics ===")
        print({k: float(v) for k, v in metrics.items()})

    if plot_confusion:
        print("\n=== Global Confusion Matrix ===")
        plot_confusion_matrix(y_true_all, y_pred_all)

    return metrics


def search_model_optuna(
    model_builder,
    param_space_fn,
    X,
    y,
    tscv,
    n_trials=50,
    verbose=True,
):
    """Perform hyperparameter search using Optuna (Bayesian optimization).

    Optimizes global OOF PR-AUC for consistency with benchmark_model.

    Args:
        model_builder: Callable(params) -> model instance.
        param_space_fn: Function(trial) -> dict of params.
        X: Feature matrix.
        y: Target vector.
        tscv: TimeSeriesSplit object.
        n_trials: Number of optimization trials.
        verbose: Whether to print results.

    Returns:
        Best trained model.
    """

    def objective(trial):
        params = param_space_fn(trial)

        y_true_all = []
        y_proba_all = []

        for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
            model = model_builder(**params)

            X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
            y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

            model.fit(X_tr, y_tr)
            y_proba = model.predict_proba(X_val)[:, 1]

            y_true_all.extend(y_val)
            y_proba_all.extend(y_proba)

            current_score = average_precision_score(
                np.array(y_true_all),
                np.array(y_proba_all),
            )

            # Pruning based on current global OOF PR-AUC
            trial.report(current_score, step=fold)
            if trial.should_prune():
                raise optuna.exceptions.TrialPruned()

        final_score = average_precision_score(
            np.array(y_true_all),
            np.array(y_proba_all),
        )

        return final_score

    study = optuna.create_study(
        direction="maximize",
        pruner=optuna.pruners.MedianPruner(),
    )

    study.optimize(objective, n_trials=n_trials)

    if verbose:
        print("\n=== OPTUNA RESULTS ===")
        print("Best trial:", study.best_trial.number)
        print("Best score (PR-AUC):", study.best_value)
        print("Best params:", study.best_params)

    best_model = model_builder(**study.best_params)
    best_model.fit(X, y)

    return best_model
