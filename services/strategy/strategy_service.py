import argparse
import json
import logging
import os
from dataclasses import dataclass
from typing import List, Optional, Dict

import pandas as pd
import psycopg2

from pubsub_wrapper import PubSubClient, load_config, configure_json_logger

try:  # optional dependency for ADX calculation
    import talib  # type: ignore
except Exception:  # pragma: no cover
    from types import SimpleNamespace

    talib = SimpleNamespace()

configure_json_logger()
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")

parser = argparse.ArgumentParser(description="Strategy service")
parser.add_argument("-strategy", help="strategy name")
args, _ = parser.parse_known_args()

STRATEGY_NAME = args.strategy or os.getenv("STRATEGY_NAME", "trend_follow_confirmation")
config = load_config(ENV)

DB_CONFIG = {
    "dbname": config["PGDATABASE"],
    "user": config["PGUSER"],
    "password": config["PGPASSWORD"],
    "host": config["PGHOST"],
    "port": int(config["PGPORT"]),
}

bus = PubSubClient(config.get("redis_url"))
LOOKBACK_ROWS = 250


def fetch_recent_ohlcv(ticker: str, interval: str, limit: int = LOOKBACK_ROWS) -> pd.DataFrame:
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
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
    return df.sort_values("ts").reset_index(drop=True)


def fetch_latest(table: str, ticker: str, interval: str) -> Optional[Dict[str, float]]:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        f"SELECT * FROM {table} WHERE ticker = %s AND interval = %s ORDER BY ts DESC LIMIT 1",
        (ticker, interval),
    )
    row = cur.fetchone()
    desc = [d[0] for d in cur.description] if cur.description else []
    cur.close()
    conn.close()
    if not row:
        return None
    return dict(zip(desc, row))


class BaseStrategy:
    name: str = "base"

    def __init__(self, db_config: dict):
        self.db_config = db_config

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        raise NotImplementedError


class TrendFollowConfirmation(BaseStrategy):
    name = "trend_follow_confirmation"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval)
        if df.empty:
            return []
        price = df["close"].iloc[-1]
        sma = df["close"].rolling(50).mean().iloc[-1]
        if pd.isna(sma):
            return []
        macd = fetch_latest("stock_ta_macd", ticker, interval)
        if not macd:
            return []
        macd_val = macd.get("macd")
        signal = macd.get("macd_signal")
        if macd_val is None or signal is None or pd.isna(macd_val) or pd.isna(signal):
            return []
        if price > sma and macd_val > signal:
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if price < sma and macd_val < signal:
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


class RSIPullback(BaseStrategy):
    name = "rsi_pullback"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval)
        if df.empty:
            return []
        price = df["close"].iloc[-1]
        sma200 = df["close"].rolling(200).mean().iloc[-1]
        if pd.isna(sma200):
            return []
        rsi = fetch_latest("stock_ta_rsi", ticker, interval)
        if not rsi:
            return []
        rsi_val = rsi.get("rsi")
        if rsi_val is None or pd.isna(rsi_val):
            return []
        if price > sma200 and rsi_val < 30:
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        return []


class MACDRSIStrategy(BaseStrategy):
    name = "macd_rsi"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        macd = fetch_latest("stock_ta_macd", ticker, interval)
        rsi = fetch_latest("stock_ta_rsi", ticker, interval)
        if not macd or not rsi:
            return []
        cross = macd.get("macd_crossover_type")
        rsi_val = rsi.get("rsi")
        if rsi_val is None or pd.isna(rsi_val):
            return []
        if cross == "bullish" and rsi_val > 30:
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if cross == "bearish" and rsi_val < 70:
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


class BollingerMomentum(BaseStrategy):
    name = "bollinger_momentum"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval)
        bb = fetch_latest("stock_ta_bollinger_bands", ticker, interval)
        rsi = fetch_latest("stock_ta_rsi", ticker, interval)
        if df.empty or not bb or not rsi:
            return []
        price = df["close"].iloc[-1]
        rsi_val = rsi.get("rsi")
        bb_upper = bb.get("bb_upper")
        bb_lower = bb.get("bb_lower")
        if (
            rsi_val is None
            or pd.isna(rsi_val)
            or bb_upper is None
            or pd.isna(bb_upper)
            or bb_lower is None
            or pd.isna(bb_lower)
        ):
            return []
        if price > bb_upper and rsi_val > 50:
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if price < bb_lower and rsi_val < 50:
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


class TripleConfirmation(BaseStrategy):
    name = "triple_confirmation"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval)
        bb = fetch_latest("stock_ta_bollinger_bands", ticker, interval)
        rsi = fetch_latest("stock_ta_rsi", ticker, interval)
        if df.empty or not bb or not rsi:
            return []
        price = df["close"].iloc[-1]
        sma50 = df["close"].rolling(50).mean().iloc[-1]
        recent_high = df["high"].rolling(20).max().iloc[-1]
        recent_low = df["low"].rolling(20).min().iloc[-1]
        if pd.isna(sma50) or pd.isna(recent_high) or pd.isna(recent_low):
            return []
        rsi_val = rsi.get("rsi")
        bb_upper = bb.get("bb_upper")
        bb_lower = bb.get("bb_lower")
        if (
            rsi_val is None
            or pd.isna(rsi_val)
            or bb_upper is None
            or pd.isna(bb_upper)
            or bb_lower is None
            or pd.isna(bb_lower)
        ):
            return []
        if price > sma50 and rsi_val > 50 and price > max(recent_high, bb_upper):
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if price < sma50 and rsi_val < 50 and price < min(recent_low, bb_lower):
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


class ADXMACDStrategy(BaseStrategy):
    name = "adx_macd"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval)
        macd = fetch_latest("stock_ta_macd", ticker, interval)
        if df.empty or not macd:
            return []
        if not hasattr(talib, "ADX"):
            return []
        adx = talib.ADX(df["high"].to_numpy(dtype=float), df["low"].to_numpy(dtype=float), df["close"].to_numpy(dtype=float))
        adx_val = float(adx[-1]) if len(adx) > 0 else 0
        if adx_val <= 20:
            return []
        cross = macd.get("macd_crossover_type")
        if cross is None:
            return []
        if cross == "bullish":
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if cross == "bearish":
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


class GoldenCross(BaseStrategy):
    name = "golden_cross"

    def evaluate(self, ticker: str, interval: str) -> List[Dict[str, str]]:
        df = fetch_recent_ohlcv(ticker, interval, limit=210)
        if df.empty or len(df) < 200:
            return []
        ma50 = df["close"].rolling(50).mean()
        ma200 = df["close"].rolling(200).mean()
        if pd.isna(ma50.iloc[-1]) or pd.isna(ma50.iloc[-2]) or pd.isna(ma200.iloc[-1]) or pd.isna(ma200.iloc[-2]):
            return []
        if ma50.iloc[-1] > ma200.iloc[-1] and ma50.iloc[-2] <= ma200.iloc[-2]:
            return [{"ticker": ticker, "interval": interval, "action": "BUY"}]
        if ma50.iloc[-1] < ma200.iloc[-1] and ma50.iloc[-2] >= ma200.iloc[-2]:
            return [{"ticker": ticker, "interval": interval, "action": "SELL"}]
        return []


STRATEGIES = {
    TrendFollowConfirmation.name: TrendFollowConfirmation,
    RSIPullback.name: RSIPullback,
    MACDRSIStrategy.name: MACDRSIStrategy,
    BollingerMomentum.name: BollingerMomentum,
    TripleConfirmation.name: TripleConfirmation,
    ADXMACDStrategy.name: ADXMACDStrategy,
    GoldenCross.name: GoldenCross,
}


def get_strategy(name: str, db_config: dict) -> BaseStrategy:
    cls = STRATEGIES.get(name)
    if cls is None:
        raise ValueError(f"unsupported strategy: {name}")
    return cls(db_config)


strategy = get_strategy(STRATEGY_NAME, DB_CONFIG)


def run():
    logger.info(f"Strategy service '{strategy.name}' starting")
    pubsub = bus.subscribe("ta.updated")
    logger.info(f"Subscribed to 'ta.updated' on {config.get('redis_url')}")
    for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        event = json.loads(msg["data"])
        payload = event.get("payload", {})
        ticker = payload.get("ticker")
        interval = payload.get("interval")
        if not ticker or not interval:
            continue
        indicator = payload.get("indicator")
        signals = strategy.evaluate(ticker, interval)
        latest_ohlcv = {}
        if signals:
            ohlcv_df = fetch_recent_ohlcv(ticker, interval, limit=1)
            latest_ohlcv = (
                ohlcv_df.iloc[-1].to_dict() if not ohlcv_df.empty else {}
            )
        for sig in signals:
            event_payload = {
                **sig,
                "indicator": indicator,
                "ohlcv": latest_ohlcv,
            }
            bus.publish(
                "strategy.signal",
                f"strategy.signal.{sig['action'].lower()}",
                event_payload,
            )
            logger.info(f"Published signal {event_payload}")


if __name__ == "__main__":
    run()
