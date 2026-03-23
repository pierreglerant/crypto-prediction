"""Shared test helpers for the BTC pipeline test suite."""


def make_raw_klines(rows: int = 40) -> list[list]:
    """Build synthetic Binance kline rows for tests."""
    base_ts = 1_700_000_000_000
    klines = []

    for index in range(rows):
        open_price = 100 + index
        close_price = open_price + (2 if index % 2 == 0 else -1)
        high_price = open_price + 5
        low_price = open_price - 3
        volume = 1_000 + (index * 10)
        taker_buy_base = 400 + (index * 5)
        quote_asset_volume = volume * close_price
        taker_buy_quote = taker_buy_base * close_price
        open_time = base_ts + (index * 86_400_000)
        close_time = open_time + 86_399_999

        klines.append(
            [
                open_time,
                f"{open_price:.2f}",
                f"{high_price:.2f}",
                f"{low_price:.2f}",
                f"{close_price:.2f}",
                f"{volume:.2f}",
                close_time,
                f"{quote_asset_volume:.2f}",
                100 + index,
                f"{taker_buy_base:.2f}",
                f"{taker_buy_quote:.2f}",
                "0",
            ]
        )

    return klines
