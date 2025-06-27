import importlib
import sys
from datetime import datetime
import unittest
from unittest.mock import patch

import pandas as pd


def load_put_service():
    """Import put_service with mocked config to avoid AWS calls."""
    with patch(
        "pubsub_wrapper.load_config",
        return_value={
            "PGHOST": "",
            "PGUSER": "",
            "PGPASSWORD": "",
            "PGDATABASE": "",
            "PGPORT": "5432",
            "symbols": [],
            "redis_url": "redis://localhost:6379",
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

    def test_nan_values_are_interpolated(self):
        dates = pd.date_range("2024-01-01", periods=3)
        data = {
            "Adj Close": [1.0, 2.0, 3.0],
            "Close": [1.0, float("nan"), 3.0],
            "High": [1.0, 2.0, float("nan")],
            "Low": [float("nan"), 2.0, 3.0],
            "Open": [1.0, float("nan"), 3.0],
            "Volume": [float("nan"), 2.0, 3.0],
        }
        df = pd.DataFrame(data, index=dates)
        ps = load_put_service()
        with patch("yfinance.download", return_value=df):
            result = ps.fetch_and_store_batch(["AAPL"], "1d", {"AAPL": None})
        assert not result["AAPL"].isna().any().any()


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

class TestFillMissingValues(unittest.TestCase):
    def test_returns_input_when_empty(self):
        ps = load_put_service()
        empty = pd.DataFrame()
        assert ps.fill_missing_values(empty).empty

    def test_fills_nan_values(self):
        ps = load_put_service()
        df = pd.DataFrame({
            'Open': [1.0, float('nan'), 3.0],
            'High': [float('nan'), 2.0, 3.0],
            'Low': [1.0, 2.0, float('nan')],
            'Close': [1.0, float('nan'), 3.0],
            'Volume': [float('nan'), 2.0, 3.0],
        })
        filled = ps.fill_missing_values(df)
        assert not filled.isna().any().any()


class TestGetLatestTimestamp(unittest.TestCase):
    def test_fetches_latest_timestamp(self):
        ps = load_put_service()
        mock_conn = patch('psycopg2.connect').start()
        self.addCleanup(patch.stopall)
        cur = mock_conn.return_value.cursor.return_value
        cur.fetchone.return_value = [datetime(2024, 1, 1)]
        ts = ps.get_latest_timestamp('AAPL', '1d')
        assert ts == datetime(2024, 1, 1)
        cur.execute.assert_called_once()
        cur.close.assert_called_once()
        mock_conn.return_value.close.assert_called_once()


class TestInsertOhlcvRecords(unittest.TestCase):
    def test_inserts_rows_and_returns_count(self):
        ps = load_put_service()
        df = pd.DataFrame({'Open':[1.0],'High':[2.0],'Low':[1.5],'Close':[1.8],'Volume':[10]},
                          index=[pd.Timestamp('2024-01-01')])
        mock_conn = patch('psycopg2.connect').start()
        self.addCleanup(patch.stopall)
        cur = mock_conn.return_value.cursor.return_value
        cur.rowcount = 1
        inserted = ps.insert_ohlcv_records('AAPL','1d',df)
        assert inserted == 1
        cur.executemany.assert_called_once()
        mock_conn.return_value.commit.assert_called_once()
        cur.close.assert_called_once()
        mock_conn.return_value.close.assert_called_once()


class TestNextRunTime(unittest.TestCase):
    def test_calculates_next_minute_interval(self):
        ps = load_put_service()
        now = datetime(2024,1,1,0,7,15,tzinfo=ps.timezone.utc)
        nxt = ps.next_run_time('5m', now)
        assert nxt == datetime(2024,1,1,0,10,30,tzinfo=ps.timezone.utc)

    def test_calculates_next_hour_interval(self):
        ps = load_put_service()
        now = datetime(2024,1,1,10,30,0,tzinfo=ps.timezone.utc)
        nxt = ps.next_run_time('1h', now)
        assert nxt == datetime(2024,1,1,11,0,30,tzinfo=ps.timezone.utc)

    def test_calculates_next_day_interval(self):
        ps = load_put_service()
        now = datetime(2024,1,1,12,0,0)
        nxt = ps.next_run_time('1d', now)
        assert nxt == datetime(2024,1,2,0,0,30)

    def test_invalid_interval_raises(self):
        ps = load_put_service()
        with self.assertRaises(ValueError):
            ps.next_run_time('5x', datetime.now(ps.timezone.utc))


class TestSleepUntilNext(unittest.TestCase):
    def test_sleeps_for_computed_duration(self):
        ps = load_put_service()
        future = datetime.now(ps.timezone.utc) + pd.Timedelta(seconds=1)
        with patch.object(ps, 'next_run_time', return_value=future) as mock_next, \
             patch('time.sleep') as mock_sleep:
            ps.sleep_until_next('1d')
            mock_next.assert_called_once()
            assert mock_sleep.called


class DummyExecutor:
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        pass
    def submit(self, fn, *args, **kwargs):
        result = fn(*args, **kwargs)
        class DummyFuture:
            def result(self_inner):
                return result
        return DummyFuture()


class TestRunIntegration(unittest.TestCase):
    def test_run_fetches_and_inserts(self):
        ps = load_put_service()
        ps.symbols = [('AAPL','1d'), ('MSFT','1d')]
        data = pd.DataFrame({'Open':[1], 'High':[1], 'Low':[1], 'Close':[1], 'Volume':[1]}, index=[pd.Timestamp('2024-01-01')])
        with patch.object(ps, 'get_latest_timestamp', return_value=None) as mock_get, \
             patch.object(ps, 'fetch_and_store_batch', return_value={'AAPL': data, 'MSFT': data}) as mock_fetch, \
             patch.object(ps, 'insert_and_publish') as mock_insert, \
             patch('services.put.put_service.ThreadPoolExecutor', return_value=DummyExecutor()) as mock_exec, \
             patch('services.put.put_service.as_completed', side_effect=lambda x: x):
            ps.run('1d')
            assert mock_get.call_count == 2
            mock_fetch.assert_called_once()
            assert mock_insert.call_count == 2


class TestRunForever(unittest.TestCase):
    def test_run_forever_loops(self):
        ps = load_put_service()
        calls = {'count':0}
        def run_side_effect(interval):
            calls['count'] += 1
            if calls['count'] > 1:
                raise KeyboardInterrupt()
        with patch.object(ps, 'run', side_effect=run_side_effect) as mock_run, \
             patch.object(ps, 'sleep_until_next') as mock_sleep:
            with self.assertRaises(KeyboardInterrupt):
                ps.run_forever('1d')
            assert mock_run.call_count == 2
            assert mock_sleep.call_count >= 1
