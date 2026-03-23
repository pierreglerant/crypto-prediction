"""Build gold-level BTC market features from silver data."""

import pandas as pd


def build_gold_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived features and peak-based prediction target."""
    df = df.copy()

    # =========================
    # FEATURES
    # =========================

    # Returns
    df["return_1d"] = df["close"].pct_change()
    df["return_7d"] = df["close"].pct_change(7)

    # Volatility
    df["volatility_7d"] = df["return_1d"].rolling(7).std()
    df["volatility_30d"] = df["return_1d"].rolling(30).std()

    # Drawdown
    df["cummax"] = df["close"].cummax()
    df["drawdown"] = (df["close"] - df["cummax"]) / df["cummax"]

    # Volume normalization
    df["volume_norm"] = df["volume"] / df["volume"].rolling(30).mean()

    # Buy pressure
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
    # PEAK DETECTION (TARGET)
    # =========================

    horizon = 7
    drop_threshold = -0.10

    # Future min (crash condition)
    df["future_min"] = df["close"].rolling(horizon).min().shift(-horizon)
    df["future_drawdown"] = (df["future_min"] / df["close"]) - 1

    # Future max (local peak detection)
    df["future_max"] = df["close"].rolling(horizon).max().shift(-horizon)
    df["distance_to_peak"] = (df["future_max"] - df["close"]) / df["close"]

    # Conditions
    crash_condition = df["future_drawdown"] < drop_threshold
    peak_condition = df["distance_to_peak"] < 0.02  # proche du max

    df["target"] = crash_condition & peak_condition

    # Keep only first occurrence (event-based)
    previous_target = df["target"].shift(1, fill_value=False)
    df["target"] = df["target"] & ~previous_target

    # =========================
    # CLEAN
    # =========================

    df = df.drop(
        columns=[
            "cummax",
            "future_min",
            "future_drawdown",
            "future_max",
            "distance_to_peak",
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
