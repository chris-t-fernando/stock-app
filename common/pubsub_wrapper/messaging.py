import json
import redis  # Swap later with Kafka backend (e.g., aiokafka)
from datetime import datetime, date
from typing import Any

import pandas as pd


class PubSubClient:
    """Simple Redis-based pub/sub client."""

    def __init__(self, redis_url="redis://localhost:6379"):
        self.redis = redis.Redis.from_url(redis_url)

    @staticmethod
    def _json_default(obj: Any):
        """Helper to make pandas Timestamp and datetimes serialisable."""
        if isinstance(obj, (pd.Timestamp, datetime, date)):
            return obj.isoformat()
        if hasattr(obj, "tolist"):
            return obj.tolist()
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")

    def publish(self, topic, event_type, payload, metadata=None):
        metadata = metadata or {}
        event = {
            "event_type": event_type,
            "payload": payload,
            "metadata": {
                **metadata,
                "source": __name__,
            },
        }
        self.redis.publish(topic, json.dumps(event, default=self._json_default))

    def subscribe(self, topic):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(topic)
        return pubsub
