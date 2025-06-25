import os
import logging
import json
import argparse
import pandas as pd
import psycopg2
import talib

from pubsub_wrapper import PubSubClient, load_config

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


def get_latest_macd_ts(ticker: str, interval: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM stock_ta_macd WHERE ticker = %s AND interval = %s",
        (ticker, interval),
    )
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result


def fetch_recent_closes(
    ticker: str, interval: str, limit: int = LOOKBACK_ROWS
) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ts, close FROM stock_ohlcv
        WHERE ticker = %s AND interval = %s
        ORDER BY ts DESC LIMIT %s
        """,
        (ticker, interval, limit),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return pd.DataFrame(columns=["ts", "close"])
    df = (
        pd.DataFrame(rows, columns=["ts", "close"])
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return df


def calculate_macd(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    if talib is None:
        raise ImportError("talib library is required to compute MACD")

    macd, signal, hist = talib.MACD(df["close"].astype(float).values)
    diff = macd - signal

    res = pd.DataFrame(
        {
            "ts": df["ts"],
            "macd": macd,
            "macd_signal": signal,
            "macd_hist": hist,
            "macd_diff": diff,
        }
    )

    res["macd_crossover"] = False
    res["macd_crossover_type"] = None

    bullish = (diff >= 0) & (pd.Series(diff).shift(1) < 0)
    bearish = (diff <= 0) & (pd.Series(diff).shift(1) > 0)
    res.loc[bullish, "macd_crossover"] = True
    res.loc[bullish, "macd_crossover_type"] = "bullish"
    res.loc[bearish, "macd_crossover"] = True
    res.loc[bearish, "macd_crossover_type"] = "bearish"

    return res


def insert_macd_records(ticker: str, interval: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    insert_query = """
        INSERT INTO stock_ta_macd (
            ticker, interval, ts, macd, macd_signal, macd_hist, macd_diff,
            macd_crossover, macd_crossover_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, interval, ts) DO UPDATE
        SET macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            macd_hist = EXCLUDED.macd_hist,
            macd_diff = EXCLUDED.macd_diff,
            macd_crossover = EXCLUDED.macd_crossover,
            macd_crossover_type = EXCLUDED.macd_crossover_type;
    """

    data = [
        (
            ticker,
            interval,
            row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
            float(row.macd) if pd.notna(row.macd) else None,
            float(row.macd_signal) if pd.notna(row.macd_signal) else None,
            float(row.macd_hist) if pd.notna(row.macd_hist) else None,
            float(row.macd_diff) if pd.notna(row.macd_diff) else None,
            bool(row.macd_crossover) if pd.notna(row.macd_crossover) else None,
            row.macd_crossover_type,
        )
        for row in df.itertuples(index=False)
    ]

    cur.executemany(insert_query, data)
    conn.commit()
    rows_inserted = cur.rowcount
    cur.close()
    conn.close()
    return rows_inserted


def process_ticker(ticker: str, interval: str) -> int:
    last_ts = get_latest_macd_ts(ticker, interval)
    price_df = fetch_recent_closes(ticker, interval)
    if price_df.empty:
        logger.info(f"⏭ No price data for {ticker} ({interval})")
        return 0

    macd_df = calculate_macd(price_df)
    if last_ts:
        macd_df = macd_df[macd_df["ts"] > last_ts]
    rows = insert_macd_records(ticker, interval, macd_df)
    logger.info(f"✅ MACD stored for {ticker} ({interval}) - {rows} new rows")
    return rows


def run():
    logger.info(f"TA service '{TA_NAME}' starting test")
    pubsub = bus.subscribe("stock.updated")
    logger.info(f"Subscribed to 'stock.updated' on {config.get('redis_url')}")
    for msg in pubsub.listen():
        logger.info(f"Received message: {msg}")
        if msg["type"] != "message":
            continue
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


if __name__ == "__main__":
    run()
