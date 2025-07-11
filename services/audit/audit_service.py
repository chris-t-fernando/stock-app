import os
import logging
import json
from pubsub_wrapper import PubSubClient, load_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

bus = PubSubClient(config.get("redis_url"))

for msg in bus.subscribe("stock.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    ticker = event["payload"]["ticker"]
    logger.info(f"Audit: Checking {ticker} for revisions")