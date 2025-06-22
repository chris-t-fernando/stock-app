import json
from unittest.mock import MagicMock, patch

from stocklib.messaging import EventBus


def test_publish_formats_event():
    mock_redis = MagicMock()
    with patch("redis.Redis.from_url", return_value=mock_redis):
        bus = EventBus("redis://example.com:6379")
        bus.publish("topic", "type", {"a": 1}, {"foo": "bar"})

    mock_redis.publish.assert_called_once()
    topic, data = mock_redis.publish.call_args.args
    assert topic == "topic"
    event = json.loads(data)
    assert event["event_type"] == "type"
    assert event["payload"] == {"a": 1}
    assert event["metadata"]["foo"] == "bar"
