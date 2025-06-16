from stocklib.messaging import EventBus
import json

bus = EventBus()

for msg in bus.subscribe("ta.updated"):
    if msg["type"] != "message":
        continue
    event = json.loads(msg["data"])
    print(f"Strategy: Received TA update {event}")
    bus.publish(
        "strategy.signal", "strategy.signal.buy", {"ticker": "AAPL", "action": "BUY"}
    )
