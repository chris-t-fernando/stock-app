import os
import logging
import json
import argparse

from stocklib.messaging import EventBus
from stocklib.config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")

parser = argparse.ArgumentParser(description="Technical analysis service")
parser.add_argument("-ta_name", help="technical indicator name")
args = parser.parse_args()

TA_NAME = args.ta_name or os.getenv("TA_NAME")
config = load_config(ENV)

if not TA_NAME:
    raise ValueError("TA name must be provided via CLI argument or TA_NAME env var")

symbols = config.get("symbols", [])
logger.info(f"TA service '{TA_NAME}' starting - subscribing to {len(symbols)} pairs")

bus = EventBus()

pubsub = bus.subscribe("stock.updated")

for msg in pubsub.listen():
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    ticker = event["payload"].get("ticker")
    interval = event["payload"].get("interval")
    logger.info(f"{TA_NAME}: analysing {ticker} ({interval})")
    bus.publish(
        "ta.updated",
        f"ta.updated.{TA_NAME}",
        {"ticker": ticker, "interval": interval, "indicator": TA_NAME, "value": 0},
    )
    break
