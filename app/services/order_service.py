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

for msg in bus.subscribe("strategy.signal"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    logger.info(f"Order: Received signal {event}")
