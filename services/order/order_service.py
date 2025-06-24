import os
import logging
from pubsub_wrapper import PubSubClient
from pubsub_wrapper.config import load_config
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ENV = os.getenv("STOCKAPP_ENV", "devtest")
config = load_config(ENV)

bus = PubSubClient(config.get("redis_url"))

for msg in bus.subscribe("strategy.signal"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    logger.info(f"Order: Received signal {event}")
