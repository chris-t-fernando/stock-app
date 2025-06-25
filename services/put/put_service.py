# app/services/put_service.py
import os
import logging
import argparse
import time
from datetime import datetime, timedelta
from pubsub_wrapper import PubSubClient, load_config
import yfinance as yf
import psycopg2
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Silence yfinance internal logging
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

parser = argparse.ArgumentParser(description="Price update service")
parser.add_argument("-interval", help="interval to process")
args, _ = parser.parse_known_args()
INTERVAL = args.interval or os.getenv("INTERVAL")

bus = PubSubClient(config.get("redis_url"))

DB_CONFIG = {
    "dbname": config["PGDATABASE"],
    "user": config["PGUSER"],
    "password": config["PGPASSWORD"],
    "host": config["PGHOST"],
    "port": int(config["PGPORT"]),
}

symbols = config["symbols"]
api_semaphore = Semaphore(1)  # Single API call at a time for yfinance stability


def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Forward/back fill NaN values for OHLCV columns."""
    if df is None or df.empty:
        return df

    df = df.copy()
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df.loc[:, col] = pd.Series(df[col]).ffill().bfill()
    return df

def get_latest_timestamp(ticker, interval):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM stock_ohlcv WHERE ticker = %s AND interval = %s",
        (ticker, interval),
    )
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result


def insert_ohlcv_records(ticker, interval, df):
    if df is None or df.empty:
        logger.info(f"⏭ Skipped (no data): {ticker} ({interval})")
        return 0

    df = fill_missing_values(df.copy())

    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index(level=0)

    df = df.reset_index()

    def extract_scalar_timestamp(row):
        for key in row.keys():
            if (isinstance(key, tuple) and key[0] in ("Datetime", "Date", "index")) or (
                isinstance(key, str) and key in ("Datetime", "Date", "index")
            ):
                ts = row[key]
                if isinstance(ts, (pd.Series, pd.DataFrame)):
                    logger.debug(f"Row[{key}] is not scalar: {ts}")
                    continue
                if pd.notna(ts):
                    ts = pd.to_datetime(ts)
                    return ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
        logger.warning(
            f"⚠️ No valid timestamp in row: {row.to_dict()} | keys: {list(row.keys())}"
        )
        raise ValueError("No valid scalar timestamp field found in row")

    def extract_field(row, field_name):
        try:
            if isinstance(row.index, pd.MultiIndex):
                return row[(field_name, "")] if (field_name, "") in row else None
            return row[field_name]
        except Exception:
            return None

    rows = []
    for _, row in df.iterrows():
        try:
            ts = extract_scalar_timestamp(row)
            open_ = extract_field(row, "Open")
            high = extract_field(row, "High")
            low = extract_field(row, "Low")
            close = extract_field(row, "Close")
            volume_val = extract_field(row, "Volume")

            rows.append(
                (
                    ticker,
                    interval,
                    ts,
                    float(open_) if pd.notna(open_) else None,
                    float(high) if pd.notna(high) else None,
                    float(low) if pd.notna(low) else None,
                    float(close) if pd.notna(close) else None,
                    (
                        int(volume_val)
                        if pd.notna(volume_val) and not pd.isnull(volume_val)
                        else None
                    ),
                )
            )
        except Exception as e:
            logger.warning(f"⚠️ Skipping row for {ticker} ({interval}): {e}")

    if not rows:
        logger.info(f"⏭ Skipped (no parsable rows): {ticker} ({interval})")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    insert_query = """
        INSERT INTO stock_ohlcv (ticker, interval, ts, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, interval, ts) DO NOTHING;
    """
    cur.executemany(insert_query, rows)
    conn.commit()
    rows_inserted = cur.rowcount
    cur.close()
    conn.close()
    return rows_inserted


def fetch_and_store_batch(tickers, interval, start_map):
    overall_start = None
    for t in tickers:
        ts = start_map.get(t)
        if ts is not None:
            if overall_start is None or ts < overall_start:
                overall_start = ts

    if overall_start is not None:
        overall_start += timedelta(minutes=1)

    logger.info(f"Fetching batch {tickers} ({interval}) starting from {overall_start}")

    try:
        with api_semaphore:
            df = yf.download(
                tickers=tickers,
                start=overall_start,
                interval=interval,
                auto_adjust=False,
                progress=False,
            )
    except Exception as e:
        logger.warning(
            f"⚠️ yfinance download issue for batch {tickers} ({interval}): {e}"
        )
        return {}

    if df is None or df.empty:
        logger.info(f"⏭ Skipped (no data): batch {tickers} ({interval})")
        return {}

    results = {}
    if df.columns.nlevels == 1:
        # yfinance omits the ticker level when only one ticker is requested
        results[tickers[0]] = fill_missing_values(df)
    else:
        for ticker in tickers:
            if ticker in df.columns.get_level_values(1):
                df_ticker = df.xs(ticker, axis=1, level=1).copy()
                results[ticker] = fill_missing_values(df_ticker)
            else:
                logger.warning(
                    f"⚠️ Ticker '{ticker}' not found in data columns for batch {tickers} ({interval})"
                )
                results[ticker] = pd.DataFrame()

    return results


def insert_and_publish(ticker, interval, df):
    rows_inserted = insert_ohlcv_records(ticker, interval, df)
    if rows_inserted > 0:
        bus.publish(
            "stock.updated",
            "stock.updated",
            {"ticker": ticker, "interval": interval, "new_rows": rows_inserted},
        )
        logger.info(
            f"✅ Success: {ticker} ({interval}) - Inserted {rows_inserted} new rows"
        )
    else:
        logger.info(f"⏭ No new rows to insert for {ticker} ({interval})")


def next_run_time(interval: str, now: datetime | None = None) -> datetime:
    """Return the next scheduled run time for the given interval."""
    now = now or datetime.utcnow()
    if interval.endswith("m"):
        step = int(interval[:-1])
        base = now.replace(second=0, microsecond=0)
        minutes = (now.minute // step) * step
        base = base.replace(minute=minutes)
        next_time = base + timedelta(minutes=step)
    elif interval.endswith("h"):
        step = int(interval[:-1])
        base = now.replace(minute=0, second=0, microsecond=0)
        hours = (now.hour // step) * step
        base = base.replace(hour=hours)
        next_time = base + timedelta(hours=step)
    elif interval.endswith("d"):
        step = int(interval[:-1])
        base = datetime(now.year, now.month, now.day)
        next_time = base + timedelta(days=step)
        if next_time <= now:
            next_time += timedelta(days=step)
    else:
        raise ValueError(f"unsupported interval: {interval}")
    if next_time <= now:
        if interval.endswith("m"):
            next_time += timedelta(minutes=step)
        elif interval.endswith("h"):
            next_time += timedelta(hours=step)
    return next_time + timedelta(seconds=30)


def sleep_until_next(interval: str):
    next_ts = next_run_time(interval)
    wait = (next_ts - datetime.utcnow()).total_seconds()
    if wait > 0:
        logger.info(f"Sleeping {wait:.1f}s until next run at {next_ts} for {interval}")
        time.sleep(wait)


def run(target_interval: str | None = None):
    interval_map: dict[str, list[str]] = {}
    for ticker, interval in symbols:
        interval_map.setdefault(interval, []).append(ticker)

    intervals = [target_interval] if target_interval else list(interval_map.keys())

    for interval in intervals:
        tickers = interval_map.get(interval, [])
        if not tickers:
            continue
        start_map = {t: get_latest_timestamp(t, interval) for t in tickers}

        batch_data = fetch_and_store_batch(tickers, interval, start_map)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for ticker in tickers:
                df_ticker = batch_data.get(ticker)
                if df_ticker is None or df_ticker.empty:
                    logger.info(f"⏭ Skipped (no data): {ticker} ({interval})")
                    continue
                futures.append(
                    executor.submit(insert_and_publish, ticker, interval, df_ticker)
                )
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"❌ Error during insert for ticker batch: {e}")


def run_forever(interval: str):
    sleep_until_next(interval)
    while True:
        run(interval)
        sleep_until_next(interval)


if __name__ == "__main__":
    if INTERVAL:
        run_forever(INTERVAL)
    else:
        run()
