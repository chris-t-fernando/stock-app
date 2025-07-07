import json
from unittest.mock import MagicMock, patch
import pandas as pd

from pubsub_wrapper.messaging import PubSubClient


def test_publish_formats_event():
    mock_redis = MagicMock()
    with patch("redis.Redis.from_url", return_value=mock_redis):
        bus = PubSubClient("redis://example.com:6379")
        ts = pd.Timestamp("2024-01-01")
        bus.publish("topic", "type", {"ts": ts}, {"foo": ts})

    mock_redis.publish.assert_called_once()
    topic, data = mock_redis.publish.call_args.args
    assert topic == "topic"
    event = json.loads(data)
    assert event["event_type"] == "type"
    assert event["payload"] == {"ts": "2024-01-01T00:00:00"}
    assert event["metadata"]["foo"] == "2024-01-01T00:00:00"
