import sys
from pathlib import Path
import importlib
from datetime import datetime
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))


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
        if "services.put_service" in sys.modules:
            module = sys.modules["services.put_service"]
            return importlib.reload(module)
        return importlib.import_module("services.put_service")


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
