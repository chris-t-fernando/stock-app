import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np

def load_ta_service():
    with patch(
        "pubsub_wrapper.load_config",
        return_value={
            "PGHOST": "",
            "PGUSER": "",
            "PGPASSWORD": "",
            "PGDATABASE": "",
            "PGPORT": "5432",
            "redis_url": "redis://localhost:6379",
        },
    ):
        if "services.ta.ta_service" in sys.modules:
            return importlib.reload(sys.modules["services.ta.ta_service"])
        return importlib.import_module("services.ta.ta_service")

import sys
import importlib

class TestCalculateMACD(unittest.TestCase):
    def test_macd_crossover_detection(self):
        ts = load_ta_service()
        df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=3),
            'close': [1.0, 2.0, 3.0]
        })

        with patch.object(ts, 'talib') as mock_talib:
            mock_talib.MACD.return_value = (
                np.array([1.0, 2.0, 3.0]),
                np.array([2.0, 1.0, 1.0]),
                np.array([0.0, 0.0, 0.0])
            )
            result = ts.calculate_macd(df)

        assert result.loc[1, 'macd_crossover']
        assert result.loc[1, 'macd_crossover_type'] == 'bullish'
        assert not result.loc[2, 'macd_crossover']

    def test_empty_df_returns_empty(self):
        ts = load_ta_service()
        result = ts.calculate_macd(pd.DataFrame(columns=['ts', 'close']))
        assert result.empty

    def test_invalid_close_type_raises(self):
        ts = load_ta_service()
        df = pd.DataFrame({'ts': pd.date_range('2024-01-01', periods=2), 'close': ['a', 'b']})
        with patch.object(ts, 'talib'):
            with self.assertRaises(ValueError):
                ts.calculate_macd(df)

    def test_macd_values_not_all_nan(self):
        ts = load_ta_service()
        df = pd.DataFrame({'ts': pd.date_range('2024-01-01', periods=5), 'close': [1,2,3,4,5]})
        with patch.object(ts, 'talib') as mock_talib:
            mock_talib.MACD.return_value = (
                np.arange(5, dtype=float),
                np.arange(5, dtype=float),
                np.arange(5, dtype=float)
            )
            result = ts.calculate_macd(df)
        assert not result[['macd','macd_signal','macd_hist']].isna().all().any()

    def test_nan_rows_logged_and_skipped(self):
        ts = load_ta_service()
        df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=3),
            'close': [1.0, np.nan, 3.0]
        })

        with patch.object(ts, 'logger') as mock_logger, \
             patch.object(ts, 'talib') as mock_talib:
            mock_talib.MACD.return_value = (
                np.array([1.0, 3.0]),
                np.array([0.5, 2.5]),
                np.array([0.0, 0.0])
            )
            result = ts.calculate_macd(df)
            mock_logger.error.assert_called_once()

        assert len(result) == 2

