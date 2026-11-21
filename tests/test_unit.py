"""Fast unit tests using fake Kafka clients (no broker required)."""
import base64
import json

import pytest

from inhouse_kafka.consumer import EventWorker
from inhouse_kafka.events import build_event
from inhouse_kafka.producer import EventPublisher
from inhouse_kafka.settings import KafkaSettings
from inhouse_kafka.topics import ensure_topics


class FakeProducer:
    def __init__(self, *, error=None, remaining=0):
        self.sent = []
        self.error = error
        self.remaining = remaining

    def produce(self, **kwargs):
        self.sent.append(kwargs)

    def flush(self, timeout):
        if self.remaining == 0:
            self.sent[-1]["on_delivery"](self.error, None)
        return self.remaining


class FakeMessage:
    def __init__(self, value, *, key=b"tenant-1"):
        self._value = value
        self._key = key

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return "inhouse.events.v1"

    def partition(self):
        return 1

    def offset(self):
        return 42

    def error(self):
        return None


class FakeConsumer:
    def __init__(self):
        self.commits = []

    def commit(self, **kwargs):
        self.commits.append(kwargs)


class FakePublisher:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    def publish(self, event_type, payload, **kwargs):
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.events.append((event_type, payload, kwargs))
        return "dlq-id"


def message(event_type="order.created"):
    return FakeMessage(json.dumps(build_event(event_type, {"order_id": 7})).encode())


def worker(handler, consumer=None, publisher=None, attempts=3):
    return EventWorker(
        KafkaSettings(),
        handler,
        consumer=consumer if consumer is not None else FakeConsumer(),
        publisher=publisher if publisher is not None else FakePublisher(),
        max_attempts=attempts,
        retry_backoff_seconds=0,
        sleeper=lambda delay: None,
    )


def test_event_envelope():
    event = build_event("order.created", {"id": 1}, correlation_id="request-123")
    assert event["schema_version"] == 1
    assert event["correlation_id"] == "request-123"
    assert event["event_id"]
    assert event["occurred_at"].endswith("+00:00")
    with pytest.raises(ValueError):
        build_event("", {"id": 1})


def test_producer_publish_acks_before_returning():
    producer = FakeProducer()
    result = EventPublisher(KafkaSettings(), producer=producer).publish(
        "order.created", {"order_id": 7}, key="customer-3"
    )
    assert result
    assert producer.sent[0]["topic"] == "inhouse.events.v1"
    assert producer.sent[0]["key"] == b"customer-3"
    envelope = json.loads(producer.sent[0]["value"])
    assert envelope["event_id"] == result


@pytest.mark.parametrize("fake", [FakeProducer(error="delivery failed"), FakeProducer(remaining=1)])
def test_producer_rejects_unconfirmed_delivery(fake):
    with pytest.raises(RuntimeError, match="not confirmed"):
        EventPublisher(KafkaSettings(), producer=fake).publish("order.created", {})


def test_success_commits_only_after_handler():
    consumer = FakeConsumer()
    observed = []

    def handler(event):
        assert consumer.commits == []
        observed.append(event["event_type"])

    worker(handler, consumer=consumer).handle_message(message())
    assert observed == ["order.created"]
    assert len(consumer.commits) == 1
    assert consumer.commits[0]["asynchronous"] is False


def test_retry_succeeds_without_dead_letter():
    consumer, publisher = FakeConsumer(), FakePublisher()
    attempts = []

    def flaky(event):
        attempts.append(event)
        if len(attempts) < 3:
            raise OSError("temporary outage")

    worker(flaky, consumer, publisher).handle_message(message())
    assert len(attempts) == 3
    assert len(consumer.commits) == 1
    assert publisher.events == []


def test_failed_handler_sends_to_dlq_and_then_commits():
    consumer, publisher = FakeConsumer(), FakePublisher()

    def fail(event):
        raise ValueError("downstream rejected")

    worker(fail, consumer, publisher, attempts=2).handle_message(message())
    assert len(publisher.events) == 1
    event_type, payload, options = publisher.events[0]
    assert event_type == "integration.delivery_failed"
    assert options["topic"] == KafkaSettings().dlq_topic
    assert payload["source_offset"] == 42
    assert base64.b64decode(payload["original_key_b64"]) == b"tenant-1"
    assert len(consumer.commits) == 1


def test_dlq_failure_does_not_commit():
    consumer = FakeConsumer()

    def fail(event):
        raise ValueError("downstream failed")

    with pytest.raises(RuntimeError, match="broker unavailable"):
        worker(fail, consumer, FakePublisher(fail=True), attempts=1).handle_message(message())
    assert consumer.commits == []


def test_poison_message_dlq_and_commit():
    consumer, publisher = FakeConsumer(), FakePublisher()
    raw = FakeMessage(b"not-json")
    worker(lambda event: None, consumer, publisher).handle_message(raw)
    assert base64.b64decode(publisher.events[0][1]["original_value_b64"]) == b"not-json"
    assert len(consumer.commits) == 1


def test_security_configuration_requires_sasl_credentials():
    with pytest.raises(ValueError, match="SASL requires"):
        KafkaSettings(security_protocol="SASL_SSL").producer_config()
    config = KafkaSettings(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="service",
        sasl_password="not-a-real-password",
    ).producer_config()
    assert config["sasl.mechanism"] == "SCRAM-SHA-512"
    assert config["enable.idempotence"] is True


def test_topic_names_must_be_distinct():
    with pytest.raises(ValueError, match="differ"):
        ensure_topics(KafkaSettings(topic="same", dlq_topic="same"), admin=object())
