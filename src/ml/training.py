"""Training and evaluation utilities."""

from sklearn.model_selection import GridSearchCV

from ml.metrics import (
    calculate_means,
    calculate_metrics,
    plot_confusion_matrix,
)


def benchmark_model(
    model,
    X,
    y,
    tscv,
    threshold=0.5,
    verbose=True,
    plot_confusion=True,
):
    """Evaluate a model using time-series cross-validation.

    Only one global confusion matrix is computed across all folds.

    Args:
        model: Model instance.
        X: Feature matrix.
        y: Target vector.
        tscv: TimeSeriesSplit object.
        threshold: Decision threshold.
        verbose: Whether to print metrics.
        plot_confusion: Whether to display confusion matrix.

    Returns:
        Dictionary of averaged metrics.
    """
    metrics_list = []

    y_true_all = []
    y_pred_all = []

    for train_idx, val_idx in tscv.split(X):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model.fit(X_tr, y_tr)

        y_proba = model.predict_proba(X_val)[:, 1]
        y_pred = (y_proba > threshold).astype(int)

        y_true_all.extend(y_val)
        y_pred_all.extend(y_pred)

        metrics = calculate_metrics(y_val, y_pred, y_proba)
        metrics_list.append(metrics)

    means = calculate_means(metrics_list)

    if verbose:
        print("\n=== Mean Metrics ===")
        print(means)

    if plot_confusion:
        print("\n=== Global Confusion Matrix ===")
        plot_confusion_matrix(y_true_all, y_pred_all)

    return means


def search_model(model, param_grid, X, y, tscv, verbose=True):
    """Perform hyperparameter search using GridSearchCV."""
    grid = GridSearchCV(
        estimator=model,
        param_grid=param_grid,
        cv=tscv,
        scoring="average_precision",
        refit=True,
        n_jobs=-1,
        verbose=2 if verbose else 0,
    )

    grid.fit(X, y)

    if verbose:
        print("\n=== Grid Search Results ===")
        print("Best params:", grid.best_params_)
        print("Best score (PR-AUC):", grid.best_score_)

    return grid.best_estimator_
