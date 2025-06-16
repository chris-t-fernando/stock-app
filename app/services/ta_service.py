from stocklib.messaging import EventBus
import json

bus = EventBus()

for msg in bus.subscribe("stock.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    ticker = event["payload"]["ticker"]
    print(f"TA: Computing indicators for {ticker}")
    bus.publish(
        "ta.updated",
        "ta.updated.RSI",
        {"ticker": ticker, "indicator": "RSI", "value": 70},
    )
