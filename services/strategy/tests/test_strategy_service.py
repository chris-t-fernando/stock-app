import importlib
import sys
import unittest
from unittest.mock import patch

import pandas as pd


def load_strategy_service():
    with patch(
        "pubsub_wrapper.load_config",
        return_value={
            "PGHOST": "",
            "PGUSER": "",
            "PGPASSWORD": "",
            "PGDATABASE": "",
            "PGPORT": "5432",
            "redis_url": "redis://localhost:6379",
            "symbols": [],
        },
    ):
        if "services.strategy.strategy_service" in sys.modules:
            return importlib.reload(sys.modules["services.strategy.strategy_service"])
        return importlib.import_module("services.strategy.strategy_service")


class TestGoldenCross(unittest.TestCase):
    def test_buy_signal(self):
        ss = load_strategy_service()
        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=201),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": [1]*200 + [100],
            "volume": 1,
        })
        with patch.object(ss, "fetch_recent_ohlcv", return_value=df):
            strat = ss.GoldenCross({})
            result = strat.evaluate("AAPL", "1d")
        assert result == [{"ticker": "AAPL", "interval": "1d", "action": "BUY"}]


if __name__ == "__main__":
    unittest.main()
