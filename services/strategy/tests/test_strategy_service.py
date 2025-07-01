import importlib
import sys
import unittest
from unittest.mock import patch
import json

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


class TestRunIntegration(unittest.TestCase):
    def test_run_publishes_with_indicator_and_ohlcv(self):
        ss = load_strategy_service()
        message = {
            "type": "message",
            "data": json.dumps(
                {"payload": {"ticker": "AAPL", "interval": "1d", "indicator": "macd"}}
            ),
        }

        class DummySub:
            def listen(self_inner):
                yield message
                raise KeyboardInterrupt()

        df = pd.DataFrame(
            {
                "ts": [pd.Timestamp("2024-01-02")],
                "open": [1],
                "high": [1],
                "low": [1],
                "close": [1],
                "volume": [1],
            }
        )

        with patch.object(ss.bus, "subscribe", return_value=DummySub()) as mock_sub, \
             patch.object(ss, "fetch_recent_ohlcv", return_value=df) as mock_fetch, \
             patch.object(ss.strategy, "evaluate", return_value=[{"ticker": "AAPL", "interval": "1d", "action": "BUY"}]) as mock_eval, \
             patch.object(ss.bus, "publish") as mock_pub:
            with self.assertRaises(KeyboardInterrupt):
                ss.run()

        mock_sub.assert_called_once_with("ta.updated")
        mock_eval.assert_called_once_with("AAPL", "1d")
        mock_fetch.assert_called_once()
        mock_pub.assert_called_once()
        args = mock_pub.call_args.args
        assert args[0] == "strategy.signal"
        assert args[1] == "strategy.signal.buy"
        payload = args[2]
        assert payload["indicator"] == "macd"
        assert payload["ohlcv"] == df.iloc[-1].to_dict()


if __name__ == "__main__":
    unittest.main()
