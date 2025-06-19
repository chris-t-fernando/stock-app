import os
import logging
from stocklib.messaging import EventBus
from stocklib.config import load_config
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

bus = EventBus()

for msg in bus.subscribe("stock.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    ticker = event["payload"]["ticker"]
    logger.info(f"TA: Computing indicators for {ticker}")
    bus.publish("ta.updated", "ta.updated.RSI", {"ticker": ticker, "indicator": "RSI", "value": 70})