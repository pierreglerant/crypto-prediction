"""Pipeline orchestrating ML model tuning and benchmarking."""

from ml.config import N_SPLITS, N_TRIALS, TARGET, TEST_SIZE_RATIO
from ml.data import get_tscv, train_test_split_time
from ml.model import get_models
from ml.training import benchmark_model, search_model_optuna


def run_benchmark_pipeline(
    df,
    threshold=None,
    verbose=True,
    plot_confusion=True,
):
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

    for name, model_cfg in models.items():
        if verbose:
            print("\n====================")
            print(f"MODEL: {name}")
            print("====================")

        builder = model_cfg["builder"]
        param_space = model_cfg["param_space"]

        model = search_model_optuna(
            model_builder=builder,
            param_space_fn=param_space,
            X=X_train,
            y=y_train,
            tscv=tscv,
            n_trials=N_TRIALS,
            verbose=verbose,
        )

        metrics = benchmark_model(
            model,
            X_train,
            y_train,
            tscv,
            threshold=threshold,
            verbose=verbose,
            plot_confusion=plot_confusion,
            model_name=name,
        )

        results[name] = metrics

    return results
