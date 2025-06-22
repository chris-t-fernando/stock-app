import sys
from pathlib import Path
import importlib
from datetime import datetime
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))


def load_put_service():
    """Import put_service with mocked config to avoid AWS calls."""
    with patch(
        "stocklib.config.load_config",
        return_value={
            "PGHOST": "",
            "PGUSER": "",
            "PGPASSWORD": "",
            "PGDATABASE": "",
            "PGPORT": "5432",
            "symbols": [],
        },
    ):
        if "services.put.put_service" in sys.modules:
            module = sys.modules["services.put.put_service"]
            return importlib.reload(module)
        return importlib.import_module("services.put.put_service")


def make_multi_index_df():
    dates = pd.date_range("2024-01-01", periods=2)
    columns = pd.MultiIndex.from_product(
        [["Adj Close", "Close", "High", "Low", "Open", "Volume"], ["AAPL", "MSFT"]],
        names=["Price", "Ticker"],
    )
    data = {
        (col, tic): [1.0, 2.0]
        for col in ["Adj Close", "Close", "High", "Low", "Open", "Volume"]
        for tic in ["AAPL", "MSFT"]
    }
    df = pd.DataFrame(data, index=dates)
    return df


def make_single_level_df():
    dates = pd.date_range("2024-01-01", periods=2)
    data = {
        "Adj Close": [1.0, 2.0],
        "Close": [1.0, 2.0],
        "High": [1.0, 2.0],
        "Low": [1.0, 2.0],
        "Open": [1.0, 2.0],
        "Volume": [1.0, 2.0],
    }
    df = pd.DataFrame(data, index=dates)
    return df


class TestFetchAndStoreBatch(unittest.TestCase):
    def test_single_ticker_single_level_df(self):
        df = make_single_level_df()
        ps = load_put_service()
        with patch("yfinance.download", return_value=df) as mock_dl:
            result = ps.fetch_and_store_batch(
                ["AAPL"], "1d", {"AAPL": datetime(2024, 1, 1)}
            )
            mock_dl.assert_called_once()
        pd.testing.assert_frame_equal(result["AAPL"], df)

    def test_multi_ticker_multi_index_df(self):
        df = make_multi_index_df()
        ps = load_put_service()
        with patch("yfinance.download", return_value=df):
            result = ps.fetch_and_store_batch(
                ["AAPL", "MSFT"], "1d", {"AAPL": None, "MSFT": None}
            )
        pd.testing.assert_frame_equal(result["AAPL"], df.xs("AAPL", axis=1, level=1))
        pd.testing.assert_frame_equal(result["MSFT"], df.xs("MSFT", axis=1, level=1))

    def test_missing_ticker_returns_empty_df(self):
        df = make_multi_index_df().xs("AAPL", axis=1, level=1, drop_level=False)
        ps = load_put_service()
        with patch("yfinance.download", return_value=df):
            result = ps.fetch_and_store_batch(
                ["AAPL", "MSFT"], "1d", {"AAPL": None, "MSFT": None}
            )
        assert result["MSFT"].empty


class TestInsertAndPublish(unittest.TestCase):
    def test_publish_on_successful_insert(self):
        ps = load_put_service()
        df = make_single_level_df()
        with patch.object(ps, "insert_ohlcv_records", return_value=2) as mock_insert, \
             patch.object(ps.bus, "publish") as mock_pub:
            ps.insert_and_publish("AAPL", "1d", df)
            mock_insert.assert_called_once()
            mock_pub.assert_called_once()
            topic, event_type, payload = mock_pub.call_args.args
            assert topic == "stock.updated"
            assert event_type == "stock.updated"
            assert payload == {"ticker": "AAPL", "interval": "1d", "new_rows": 2}

    def test_no_publish_when_no_rows_inserted(self):
        ps = load_put_service()
        with patch.object(ps, "insert_ohlcv_records", return_value=0), \
             patch.object(ps.bus, "publish") as mock_pub:
            ps.insert_and_publish("AAPL", "1d", make_single_level_df())
            mock_pub.assert_not_called()

    def test_no_publish_when_insert_fails(self):
        ps = load_put_service()
        with patch.object(ps, "insert_ohlcv_records", side_effect=Exception("db error")), \
             patch.object(ps.bus, "publish") as mock_pub:
            with self.assertRaises(Exception):
                ps.insert_and_publish("AAPL", "1d", make_single_level_df())
            mock_pub.assert_not_called()

    def test_publish_error_propagates(self):
        ps = load_put_service()
        with patch.object(ps, "insert_ohlcv_records", return_value=1), \
             patch.object(ps.bus, "publish", side_effect=Exception("publish fail")) as mock_pub:
            with self.assertRaises(Exception):
                ps.insert_and_publish("AAPL", "1d", make_single_level_df())
            mock_pub.assert_called_once()
