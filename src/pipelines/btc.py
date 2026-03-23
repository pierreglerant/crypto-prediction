"""Run the BTC/USDT data pipeline from ingestion to gold features."""

from ingestion.btc import fetch_full_history
from processing.btc.gold import build_gold_features
from processing.btc.silver import klines_to_dataframe
from utils.config import CONFIG
from utils.io import save_data


def main():
    """Execute the bronze, silver, and gold data pipeline."""
    symbol = CONFIG["symbol"]
    interval = CONFIG["interval"]
    start_date = CONFIG["start_date"]

    raw_data = fetch_full_history(symbol, interval, start_date)

    # Bronze
    save_data(raw_data, "bronze", suffix="_raw", is_json=True)

    # Silver
    df_silver = klines_to_dataframe(raw_data)
    save_data(df_silver, "silver")

    # Gold
    df_gold = build_gold_features(df_silver)
    save_data(df_gold, "gold", suffix="_features")

    print(f"Rows fetched: {len(raw_data)}")
    print(f"Gold shape: {df_gold.shape}")


if __name__ == "__main__":
    main()
