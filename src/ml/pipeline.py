"""Main pipeline orchestrating model training and evaluation."""

from ml.config import N_SPLITS, PARAM_GRIDS, TARGET, TEST_SIZE_RATIO, THRESHOLD
from ml.data import get_tscv, train_test_split_time
from ml.model import get_models
from ml.training import benchmark_model, search_model


def run_pipeline(df, verbose=True, plot_confusion=True):
    """Run the full ML benchmark pipeline.

    Args:
        df: Input dataframe containing features and target.
        verbose: Whether to print detailed logs.
        plot_confusion: Whether to display confusion matrix.

    Returns:
        Dictionary of model performance metrics.
    """
    X = df.drop(columns=[TARGET])
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split_time(X, y, TEST_SIZE_RATIO)

    tscv = get_tscv(N_SPLITS)

    models = get_models()

    results = {}

    for name, model in models.items():
        if verbose:
            print("\n====================")
            print(f"MODEL: {name}")
            print("====================")

        if name in PARAM_GRIDS:
            model = search_model(
                model,
                PARAM_GRIDS[name],
                X_train,
                y_train,
                tscv,
                verbose=verbose,
            )

        metrics = benchmark_model(
            model,
            X_train,
            y_train,
            tscv,
            threshold=THRESHOLD,
            verbose=verbose,
            plot_confusion=plot_confusion,
        )

        results[name] = metrics

    return results
