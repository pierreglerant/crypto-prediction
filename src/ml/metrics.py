"""Evaluation metrics and visualization utilities for classification models."""

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)


def calculate_metrics(y_true, y_pred, y_proba=None):
    """Compute classification metrics.

    Args:
        y_true: Ground truth labels.
        y_pred: Predicted labels.
        y_proba: Predicted probabilities for the positive class.

    Returns:
        Dictionary of metrics.
    """
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)

    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()

    metrics = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
    }

    if y_proba is not None:
        metrics["pr_auc"] = average_precision_score(y_true, y_proba)

    return metrics


def calculate_means(metrics_list):
    """Compute mean values of metrics across folds.

    Args:
        metrics_list: List of metric dictionaries.

    Returns:
        Dictionary of averaged metrics.
    """
    return {key: float(np.nanmean([m[key] for m in metrics_list])) for key in metrics_list[0]}


def plot_confusion_matrix(y_true, y_pred, title="Confusion Matrix"):
    """Plot a confusion matrix using seaborn heatmap.

    Args:
        y_true: Ground truth labels.
        y_pred: Predicted labels.
        title: Title of the plot.
    """
    cm = confusion_matrix(y_true, y_pred)

    plt.figure(figsize=(5, 4))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")

    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title(title)

    plt.tight_layout()
    plt.show()
