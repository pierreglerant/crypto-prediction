"""Build gold-level BTC market features from silver data."""

import pandas as pd


def build_gold_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived features and peak-based prediction target."""
    df = df.copy()

    # =========================
    # FEATURES
    # =========================

    df["return_1d"] = df["close"].pct_change()
    df["return_7d"] = df["close"].pct_change(7)

    df["volatility_7d"] = df["return_1d"].rolling(7).std()
    df["volatility_30d"] = df["return_1d"].rolling(30).std()

    df["cummax"] = df["close"].cummax()
    df["drawdown"] = (df["close"] - df["cummax"]) / df["cummax"]

    df["volume_norm"] = df["volume"] / df["volume"].rolling(30).mean()
    df["buy_pressure"] = df["taker_buy_base_volume"] / df["volume"]

    # =========================
    # LAGS
    # =========================

    df["lag_return_1d"] = df["return_1d"].shift(1)
    df["lag_return_7d"] = df["return_7d"].shift(1)
    df["lag_volatility_7d"] = df["volatility_7d"].shift(1)
    df["lag_volume_norm"] = df["volume_norm"].shift(1)
    df["lag_buy_pressure"] = df["buy_pressure"].shift(1)

    # =========================
    # TARGET (IMPROVED PEAK)
    # =========================

    horizon = 7
    drop_threshold = -0.10
    window = 3  # taille du voisinage pour max local

    # Future crash condition
    df["future_min"] = df["close"].rolling(horizon).min().shift(-horizon)
    df["future_drawdown"] = (df["future_min"] / df["close"]) - 1
    crash_condition = df["future_drawdown"] < drop_threshold

    # Local max condition (symétrique)
    rolling_max_past = df["close"].rolling(window, center=False).max()
    rolling_max_future = df["close"].shift(-window + 1).rolling(window).max()

    is_local_max = (df["close"] >= rolling_max_past) & (df["close"] >= rolling_max_future)

    # Combine conditions
    df["target"] = crash_condition & is_local_max

    # Keep only strongest peak in cluster
    df["target"] = df["target"] & (df["close"] == df["close"].rolling(window * 2 + 1, center=True).max())

    # =========================
    # CLEAN
    # =========================

    df = df.drop(
        columns=[
            "cummax",
            "future_min",
            "future_drawdown",
        ]
    )

    df = df.dropna(
        subset=[
            "return_1d",
            "volatility_7d",
            "target",
        ]
    )

    return df.reset_index(drop=True)
