import boto3
import os
import logging
from stocklib.messaging import EventBus
import yfinance as yf
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

bus = EventBus()

from stocklib.config import load_config

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

DB_CONFIG = {
    "dbname": config["PGDATABASE"],
    "user": config["PGUSER"],
    "password": config["PGPASSWORD"],
    "host": config["PGHOST"],
    "port": int(config["PGPORT"])
}


def get_latest_timestamp(ticker, interval):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM stock_ohlcv WHERE ticker = %s AND interval = %s",
        (ticker, interval)
    )
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result


def insert_ohlcv_records(ticker, interval, df):
    if df.empty:
        return

    df = df.reset_index()
    rows = [
        (
            ticker,
            interval,
            row['Datetime'].to_pydatetime() if 'Datetime' in row else row['Date'].to_pydatetime(),
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume']
        )
        for _, row in df.iterrows()
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    insert_query = """
        INSERT INTO stock_ohlcv (ticker, interval, ts, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, interval, ts) DO NOTHING;
    """
    cur.executemany(insert_query, rows)
    conn.commit()
    cur.close()
    conn.close()


def fetch_and_store(ticker, interval):
    start = get_latest_timestamp(ticker, interval)
    if start is not None:
        start += timedelta(minutes=1)  # avoid duplicate overlap
    logger.info(f"Fetching {ticker} ({interval}) starting from {start}")

    df = yf.download(ticker, start=start, interval=interval)
    insert_ohlcv_records(ticker, interval, df)
    bus.publish("stock.updated", "stock.updated", {
        "ticker": ticker,
        "interval": interval,
        "new_rows": len(df)
    })


def run():
    fetch_and_store("AAPL", "1d")

if __name__ == "__main__":
    run()