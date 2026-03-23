"""Build gold-level BTC market features from silver data."""

import pandas as pd


def build_gold_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create derived features and prediction targets."""
    df = df.copy()

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

    # Lags
    df["lag_return_1d"] = df["return_1d"].shift(1)
    df["lag_return_7d"] = df["return_7d"].shift(1)
    df["lag_volatility_7d"] = df["volatility_7d"].shift(1)
    df["lag_volume_norm"] = df["volume_norm"].shift(1)
    df["lag_buy_pressure"] = df["buy_pressure"].shift(1)

    # Target
    df["future_min"] = df["close"].rolling(7).min().shift(-7)
    df["future_drawdown"] = (df["future_min"] / df["close"]) - 1
    df["target"] = df["future_drawdown"] < -0.10

    # Clean (no leakage)
    df = df.drop(columns=["cummax", "future_min", "future_drawdown"])
    df = df.dropna(subset=["return_1d", "volatility_7d", "target"])

    return df.reset_index(drop=True)
