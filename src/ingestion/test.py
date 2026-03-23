from binance_client import BinanceClient
from datetime import datetime


def format_ts(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")


def main():
    client = BinanceClient()

    # 🔧 Paramètres test
    symbol = "BTCUSDT"
    interval = "1d"

    start = client.to_unix_ms("2020-01-01")
    end = client.to_unix_ms("2024-01-01")

    print("Fetching data from Binance...")
    data = client.get_historical_klines(
        symbol=symbol,
        interval=interval,
        start_time=start,
        end_time=end,
    )

    print(f"\n✅ Nombre de bougies récupérées: {len(data)}")

    # 🔍 Vérification contenu
    if data:
        first = data[0]
        last = data[-1]

        print("\n📊 First candle:")
        print({
            "date": format_ts(first[0]),
            "open": first[1],
            "high": first[2],
            "low": first[3],
            "close": first[4],
        })

        print("\n📊 Last candle:")
        print({
            "date": format_ts(last[0]),
            "open": last[1],
            "high": last[2],
            "low": last[3],
            "close": last[4],
        })

    # 🧠 sanity check
    assert len(data) > 1000, "❌ Trop peu de données récupérées"

    print("\n🚀 Test OK: historique récupéré avec succès")


if __name__ == "__main__":
    main()