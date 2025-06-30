import importlib
import sys
import unittest
from unittest.mock import patch

import pandas as pd
import json
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
            "symbols": [],
        },
    ):
        if "services.ta.ta_service" in sys.modules:
            return importlib.reload(sys.modules["services.ta.ta_service"])
        return importlib.import_module("services.ta.ta_service")


class TestAlgorithms(unittest.TestCase):
    def test_macd_crossover_detection(self):
        from services.ta.algorithms.macd import MACD, talib as macd_talib

        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=3),
            "close": [1.0, 2.0, 3.0],
        })

        with patch.object(macd_talib, "MACD", return_value=(
            np.array([1.0, 2.0, 3.0]),
            np.array([2.0, 1.0, 1.0]),
            np.array([0.0, 0.0, 0.0]),
        )):
            algo = MACD({})
            result = algo.calculate(df)

        assert result.loc[1, "macd_crossover"]
        assert result.loc[1, "macd_crossover_type"] == "bullish"
        assert not result.loc[2, "macd_crossover"]

    def test_rsi(self):
        from services.ta.algorithms.rsi import RSI, talib as rsi_talib

        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=2),
            "close": [1.0, 2.0],
        })

        with patch.object(rsi_talib, "RSI", return_value=np.array([50.0, 60.0]), create=True):
            algo = RSI({})
            result = algo.calculate(df)

        pd.testing.assert_series_equal(result["rsi"], pd.Series([50.0, 60.0]), check_names=False)

    def test_sma(self):
        from services.ta.algorithms.sma import SMA, talib as sma_talib

        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=2),
            "close": [1.0, 2.0],
        })

        with patch.object(sma_talib, "SMA", return_value=np.array([1.0, 1.5]), create=True):
            algo = SMA({})
            result = algo.calculate(df)

        pd.testing.assert_series_equal(result["sma"], pd.Series([1.0, 1.5]), check_names=False)

    def test_bbands(self):
        from services.ta.algorithms.bollinger_bands import BollingerBands, talib as bb_talib

        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=2),
            "close": [1.0, 2.0],
        })

        with patch.object(bb_talib, "BBANDS", return_value=(
            np.array([2.0, 3.0]),
            np.array([1.5, 2.5]),
            np.array([1.0, 2.0]),
        ), create=True):
            algo = BollingerBands({})
            result = algo.calculate(df)

        pd.testing.assert_series_equal(result["bb_upper"], pd.Series([2.0, 3.0]), check_names=False)
        pd.testing.assert_series_equal(result["bb_middle"], pd.Series([1.5, 2.5]), check_names=False)
        pd.testing.assert_series_equal(result["bb_lower"], pd.Series([1.0, 2.0]), check_names=False)

    def test_obv(self):
        from services.ta.algorithms.obv import OBV, talib as obv_talib

        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=2),
            "close": [1.0, 2.0],
            "volume": [10, 20],
        })

        with patch.object(obv_talib, "OBV", return_value=np.array([5, 10]), create=True):
            algo = OBV({})
            result = algo.calculate(df)

        pd.testing.assert_series_equal(result["obv"], pd.Series([5, 10]), check_names=False)


class TestTAService(unittest.TestCase):
    def test_process_ticker_delegates(self):
        ts = load_ta_service()
        df = pd.DataFrame({
            "ts": pd.date_range("2024-01-01", periods=2),
            "open": [1, 1],
            "high": [1, 1],
            "low": [1, 1],
            "close": [1, 1],
            "volume": [1, 1],
        })

        with patch.object(ts, "fetch_recent_ohlcv", return_value=df) as mock_fetch, \
             patch.object(ts.algorithm, "process", return_value=3) as mock_proc:
            rows = ts.process_ticker("AAPL", "1d")

        mock_fetch.assert_called_once()
        mock_proc.assert_called_once()
        assert rows == 3

class TestTAServiceDB(unittest.TestCase):
    def test_get_latest_ohlcv_ts(self):
        ts = load_ta_service()
        mock_conn = patch('psycopg2.connect').start()
        self.addCleanup(patch.stopall)
        cur = mock_conn.return_value.cursor.return_value
        cur.fetchone.return_value = [pd.Timestamp('2024-01-01')]
        result = ts.get_latest_ohlcv_ts('AAPL', '1d')
        cur.execute.assert_called_once()
        cur.close.assert_called_once()
        mock_conn.return_value.close.assert_called_once()
        assert result == pd.Timestamp('2024-01-01')

    def test_fetch_all_ohlcv(self):
        ts = load_ta_service()
        mock_conn = patch('psycopg2.connect').start()
        self.addCleanup(patch.stopall)
        cur = mock_conn.return_value.cursor.return_value
        cur.fetchall.return_value = [
            (pd.Timestamp('2024-01-01'), 1, 2, 1.5, 1.8, 10)
        ]
        df = ts.fetch_all_ohlcv('AAPL', '1d')
        assert df.shape == (1, 6)
        assert list(df.columns) == ['ts', 'open', 'high', 'low', 'close', 'volume']
        cur.execute.assert_called_once()
        cur.close.assert_called_once()
        mock_conn.return_value.close.assert_called_once()

    def test_fetch_recent_ohlcv(self):
        ts = load_ta_service()
        mock_conn = patch('psycopg2.connect').start()
        self.addCleanup(patch.stopall)
        cur = mock_conn.return_value.cursor.return_value
        cur.fetchall.return_value = [
            (pd.Timestamp('2024-01-02'), 1, 1, 1, 1, 1),
            (pd.Timestamp('2024-01-01'), 1, 1, 1, 1, 1)
        ]
        df = ts.fetch_recent_ohlcv('AAPL', '1d', limit=2)
        assert list(df['ts']) == [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-02')]
        cur.execute.assert_called_once()
        cur.close.assert_called_once()
        mock_conn.return_value.close.assert_called_once()

class TestTAServiceIntegration(unittest.TestCase):
    def test_process_backlog_publishes_updates(self):
        ts = load_ta_service()
        ts.config['symbols'] = [('AAPL', '1d')]
        with patch.object(ts.algorithm, 'get_latest_ts', return_value=None), \
             patch.object(ts, 'get_latest_ohlcv_ts', return_value=pd.Timestamp('2024-01-01')), \
             patch.object(ts, 'fetch_all_ohlcv', return_value=pd.DataFrame({'ts':[pd.Timestamp('2024-01-02')],
                                                                            'open':[1],'high':[1],'low':[1],'close':[1],'volume':[1]})), \
             patch.object(ts.algorithm, 'process', return_value=1) as mock_proc, \
             patch.object(ts.bus, 'publish') as mock_pub:
            ts.process_backlog()
        mock_proc.assert_called_once()
        mock_pub.assert_called_once()

    def test_run_consumes_messages(self):
        ts = load_ta_service()
        message = {'type':'message', 'data': json.dumps({'payload':{'ticker':'AAPL','interval':'1d'}})}
        class DummySub:
            def listen(self_inner):
                yield message
                raise KeyboardInterrupt()
        with patch.object(ts, 'process_backlog'), \
             patch.object(ts.bus, 'subscribe', return_value=DummySub()) as mock_sub, \
             patch.object(ts, 'process_ticker', return_value=1) as mock_proc, \
             patch.object(ts.bus, 'publish') as mock_pub:
            with self.assertRaises(KeyboardInterrupt):
                ts.run()
        mock_sub.assert_called_once_with('stock.updated')
        mock_proc.assert_called_once()
        mock_pub.assert_called_once()
