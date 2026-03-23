import requests
import time
from typing import List, Dict, Any, Optional


class BinanceClient:
    BASE_URL = "https://api.binance.com"

    def __init__(self, api_key: Optional[str] = None, timeout: int = 10):
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
        })

        if api_key:
            self.session.headers.update({
                "X-MBX-APIKEY": api_key
            })

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Any:
        url = f"{self.BASE_URL}{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)

            if response.status_code == 429:
                time.sleep(60)
                return self._get(endpoint, params)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Binance API error: {e}")

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> List[List[Any]]:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }

        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        return self._get("/api/v3/klines", params)

    def get_historical_klines(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        end_time: int,
    ) -> List[List[Any]]:
        all_data = []
        current_start = start_time

        while current_start < end_time:
            data = self.get_klines(
                symbol=symbol,
                interval=interval,
                start_time=current_start,
                end_time=end_time,
                limit=1000,
            )

            if not data:
                break

            all_data.extend(data)
            current_start = data[-1][0] + 1

            time.sleep(0.2)

        return all_data

    @staticmethod
    def to_unix_ms(date_str: str) -> int:
        return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")) * 1000)