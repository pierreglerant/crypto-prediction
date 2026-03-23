"""Project configuration for the BTC market data pipeline."""

CONFIG = {
    "symbol": "BTCUSDT",
    "interval": "1d",
    "start_date": "2017-01-01",
    "retry": 3,
    "sleep": 0.2,
}
