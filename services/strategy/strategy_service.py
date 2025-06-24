import os
import logging
from pubsub_wrapper import PubSubClient, load_config
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

bus = PubSubClient(config.get("redis_url"))

for msg in bus.subscribe("ta.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    logger.info(f"Strategy: Received TA update {event}")
    bus.publish("strategy.signal", "strategy.signal.buy", {"ticker": "AAPL", "action": "BUY"})
