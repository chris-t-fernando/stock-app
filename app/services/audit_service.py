from stocklib.messaging import EventBus
import json

bus = EventBus()

for msg in bus.subscribe("stock.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    ticker = event["payload"]["ticker"]
    print(f"Audit: Checking {ticker} for revisions")
