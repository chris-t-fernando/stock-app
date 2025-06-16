from stocklib.messaging import EventBus
import json

bus = EventBus()

for msg in bus.subscribe("strategy.signal"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    print(f"Order: Received signal {event}")
