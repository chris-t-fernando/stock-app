import os
import logging
import json
import argparse
import pandas as pd
import psycopg2

from pubsub_wrapper import PubSubClient, load_config
from .algorithms import get_algorithm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")

parser = argparse.ArgumentParser(description="Technical analysis service")
parser.add_argument("-ta_name", help="technical indicator name")
args, _ = parser.parse_known_args()

TA_NAME = args.ta_name or os.getenv("TA_NAME", "macd")
config = load_config(ENV)

DB_CONFIG = {
    "dbname": config["PGDATABASE"],
    "user": config["PGUSER"],
    "password": config["PGPASSWORD"],
    "host": config["PGHOST"],
    "port": int(config["PGPORT"]),
}
bus = PubSubClient(config.get("redis_url"))

LOOKBACK_ROWS = 200

algorithm = get_algorithm(TA_NAME, DB_CONFIG)


def get_latest_ohlcv_ts(ticker: str, interval: str):
    """Return the most recent timestamp from the price table."""
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


def fetch_all_ohlcv(ticker: str, interval: str) -> pd.DataFrame:
    """Fetch all OHLCV data for a ticker/interval ordered by timestamp."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT ts, open, high, low, close, volume FROM stock_ohlcv WHERE ticker = %s AND interval = %s ORDER BY ts",
        (ticker, interval),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df


def fetch_recent_ohlcv(
    ticker: str, interval: str, limit: int = LOOKBACK_ROWS
) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts, open, high, low, close, volume FROM stock_ohlcv
        WHERE ticker = %s AND interval = %s
        ORDER BY ts DESC LIMIT %s
        """,
        (ticker, interval, limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
    df = (
        pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return df




def process_ticker(ticker: str, interval: str) -> int:
    price_df = fetch_recent_ohlcv(ticker, interval)
    if price_df.empty:
        logger.info(f"⏭ No price data for {ticker} ({interval})")
        return 0

    rows = algorithm.process(ticker, interval, price_df)
    logger.info(
        f"✅ {algorithm.name.upper()} stored for {ticker} ({interval}) - {rows} new rows"
    )
    return rows


def process_backlog():
    """Process any OHLCV rows not yet analysed for all configured tickers."""
    symbols = config.get("symbols", [])
    for ticker, interval in symbols:
        latest_ta_ts = algorithm.get_latest_ts(ticker, interval)
        latest_price_ts = get_latest_ohlcv_ts(ticker, interval)

        if latest_price_ts is None:
            continue
        if latest_ta_ts and latest_ta_ts >= latest_price_ts:
            continue

        price_df = fetch_all_ohlcv(ticker, interval)
        if price_df.empty:
            continue

        price_df = price_df.iloc[LOOKBACK_ROWS:]
        rows = algorithm.process(ticker, interval, price_df)
        if rows > 0:
            logger.info(
                f"✅ Processed backlog for {ticker} ({interval}) - {rows} rows"
            )
            bus.publish(
                "ta.updated",
                f"ta.updated.{TA_NAME}",
                {
                    "ticker": ticker,
                    "interval": interval,
                    "indicator": TA_NAME,
                    "new_rows": rows,
                },
            )


def run():
    logger.info(f"TA service '{TA_NAME}' starting test")
    process_backlog()
    pubsub = bus.subscribe("stock.updated")
    logger.info(f"Subscribed to 'stock.updated' on {config.get('redis_url')}")
    for msg in pubsub.listen():
        logger.info(f"Received message: {msg}")
        if msg["type"] != "message":
            continue
        logger.debug(f"Received message: {msg}")
        event = json.loads(msg["data"])
        ticker = event["payload"].get("ticker")
        interval = event["payload"].get("interval")
        logger.info(f"{TA_NAME}: analysing {ticker} ({interval})")
        new_rows = process_ticker(ticker, interval)
        if new_rows > 0:
            bus.publish(
                "ta.updated",
                f"ta.updated.{TA_NAME}",
                {
                    "ticker": ticker,
                    "interval": interval,
                    "indicator": TA_NAME,
                    "new_rows": new_rows,
                },
            )
            logger.debug(f"Pushed update to ta.updated: {TA_NAME} {ticker} {interval}")


if __name__ == "__main__":
    run()
