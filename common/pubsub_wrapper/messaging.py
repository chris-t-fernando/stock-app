import json
import redis  # Swap later with Kafka backend (e.g., aiokafka)


class PubSubClient:
    """Simple Redis-based pub/sub client."""

    def __init__(self, redis_url="redis://localhost:6379"):
        self.redis = redis.Redis.from_url(redis_url)

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
        self.redis.publish(topic, json.dumps(event))

    def subscribe(self, topic):
        pubsub = self.redis.pubsub()
        pubsub.subscribe(topic)
        return pubsub
