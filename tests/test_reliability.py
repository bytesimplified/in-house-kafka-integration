"""No-broker tests for retries, safe replay, and atomic deduplication."""
import base64
import json
import sqlite3
from dataclasses import replace

import pytest

from inhouse_kafka.consumer import EventWorker, PermanentProcessingError
from inhouse_kafka.events import build_event
from inhouse_kafka.inbox import SQLiteInbox
from inhouse_kafka.replay import DLQReplayer
from inhouse_kafka.settings import KafkaSettings
from inhouse_kafka.topics import ensure_topics


class Msg:
    def __init__(self, event, *, topic="inhouse.events.v1", partition=0, offset=8,
                 key=b"tenant-10", headers=None):
        self._value = event if isinstance(event, bytes) else json.dumps(event).encode()
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._headers = headers or []

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def headers(self):
        return self._headers

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.commits = []
        self.paused = []
        self.resumed = []
        self.subscribed = []
        self.closed = False

    def commit(self, **kwargs):
        self.commits.append(kwargs)

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def pause(self, partitions):
        self.paused.extend(partitions)

    def resume(self, partitions):
        self.resumed.extend(partitions)

    def subscribe(self, topics, **kwargs):
        self.subscribed = topics

    def close(self):
        self.closed = True


class FakePublisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.raw = []
        self.events = []
        self.consumer = None

    def publish_raw(self, topic, value, *, key=None, headers=None):
        if self.fail:
            raise RuntimeError("broker down")
        if self.consumer is not None:
            assert not self.consumer.commits, "must broker-ack before committing"
        self.raw.append((topic, value, key, headers))

    def publish(self, event_type, payload, *, topic=None, **kwargs):
        if self.fail:
            raise RuntimeError("broker down")
        if self.consumer is not None:
            assert not self.consumer.commits, "must DLQ-ack before committing"
        self.events.append((event_type, payload, topic))
        return "confirmed"


def topic_settings(**kwargs):
    return replace(
        KafkaSettings(retry_mode="topics", retry_delays_seconds=(2.0, 4.0)),
        **kwargs,
    )


def test_topic_failure_forwards_original_event_bytes_and_key_before_commit():
    settings = topic_settings()
    event = build_event("order.created", {"id": 10})
    msg = Msg(event, headers=[("traceparent", b"abc")])
    consumer = FakeConsumer()
    publisher = FakePublisher()
    publisher.consumer = consumer

    def fail(event):
        raise OSError("temporary failure")

    worker = EventWorker(settings, fail, consumer=consumer, publisher=publisher, clock_ms=lambda: 1000)
    worker.handle_message(msg)
    assert len(consumer.commits) == 1
    topic, raw, key, headers = publisher.raw[0]
    assert topic == settings.retry_topics[0]
    assert raw == msg.value()
    assert key == b"tenant-10"
    assert json.loads(raw)["event_id"] == event["event_id"]
    assert dict(headers)["ih-retry-attempt"] == b"1"
    assert dict(headers)["ih-not-before-ms"] == b"3000"
    assert dict(headers)["traceparent"] == b"abc"


def test_next_stage_and_exhaustion_routes_to_dlq():
    settings = topic_settings()
    consumer = FakeConsumer()
    publisher = FakePublisher()
    event = build_event("order.created", {"id": 11})

    def fail(event):
        raise ConnectionError("network unavailable")

    worker = EventWorker(settings, fail, consumer=consumer, publisher=publisher, clock_ms=lambda: 1000)
    msg1 = Msg(event, topic=settings.retry_topics[0], headers=[("ih-retry-attempt", b"1")])
    worker.handle_message(msg1)
    assert publisher.raw[0][0] == settings.retry_topics[1]
    assert dict(publisher.raw[0][3])["ih-retry-attempt"] == b"2"
    msg2 = Msg(event, topic=settings.retry_topics[1], headers=[("ih-retry-attempt", b"2")])
    worker.handle_message(msg2)
    assert len(consumer.commits) == 2
    _, dlq, topic = publisher.events[0]
    assert topic == settings.dlq_topic
    assert dlq["original_event_id"] == event["event_id"]
    assert dlq["retry_attempt"] == 2
    assert base64.b64decode(dlq["original_value_b64"]) == msg2.value()


def test_permanent_failure_bypasses_retry_stages():
    settings = topic_settings()
    consumer, publisher = FakeConsumer(), FakePublisher()

    def reject(event):
        raise PermanentProcessingError("do not retry")

    EventWorker(settings, reject, consumer=consumer, publisher=publisher).handle_message(
        Msg(build_event("order.created", {}))
    )
    assert publisher.raw == []
    assert len(publisher.events) == 1
    assert len(consumer.commits) == 1


def test_failed_retry_publish_leaves_original_offset_uncommitted():
    consumer, publisher = FakeConsumer(), FakePublisher(fail=True)

    def fail(event):
        raise OSError("retry")

    worker = EventWorker(topic_settings(), fail, consumer=consumer, publisher=publisher)
    with pytest.raises(RuntimeError, match="broker down"):
        worker.handle_message(Msg(build_event("order.created", {})))
    assert consumer.commits == []


def test_retry_partition_is_paused_until_due_while_source_keeps_processing():
    settings = topic_settings()
    now = [1000]
    event = build_event("order.created", {"id": 12})
    retry_msg = Msg(
        event, topic=settings.retry_topics[0],
        headers=[("ih-retry-attempt", b"1"), ("ih-not-before-ms", b"3000")],
    )
    original = Msg(build_event("order.created", {"id": 13}))
    consumer = FakeConsumer([retry_msg, original])
    observed = []
    worker = EventWorker(
        settings, lambda event: observed.append(event["payload"]["id"]),
        consumer=consumer, publisher=FakePublisher(), clock_ms=lambda: now[0],
    )
    assert worker.poll_once() is False
    assert len(consumer.paused) == 1
    assert consumer.commits == []
    assert worker.poll_once() is True
    assert observed == [13]
    now[0] = 3000
    assert worker.poll_once() is True
    assert observed == [13, 12]
    assert len(consumer.resumed) == 1
    assert len(consumer.commits) == 2


def test_revoke_discards_paused_message_for_other_group_member():
    settings = topic_settings()
    consumer = FakeConsumer([
        Msg(build_event("event", {}), topic=settings.retry_topics[0],
            headers=[("ih-retry-attempt", b"1"), ("ih-not-before-ms", b"9000")])
    ])
    worker = EventWorker(settings, lambda e: None, consumer=consumer, publisher=FakePublisher(),
                         clock_ms=lambda: 1000)
    worker.poll_once()
    assert worker._pending
    worker._on_revoke(consumer, consumer.paused)
    assert not worker._pending


def test_invalid_retry_stage_goes_directly_to_dlq():
    settings = topic_settings()
    consumer, publisher = FakeConsumer(), FakePublisher()
    msg = Msg(build_event("event", {}), topic=settings.retry_topics[1],
              headers=[("ih-retry-attempt", b"1")])
    EventWorker(settings, lambda e: None, consumer=consumer, publisher=publisher).handle_message(msg)
    assert publisher.raw == []
    assert len(publisher.events) == 1
    assert len(consumer.commits) == 1


def test_sqlite_inbox_deduplicates_and_commits_business_effect_together(tmp_path):
    db = str(tmp_path / "inbox.db")
    inbox = SQLiteInbox(db, scope="orders")
    inbox.ensure_schema()
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE effects (event_id TEXT PRIMARY KEY, amount INTEGER)")
    event = build_event("order.created", {"amount": 99})

    def handler(event, conn):
        conn.execute("INSERT INTO effects VALUES (?,?)",
                     (event["event_id"], event["payload"]["amount"]))

    consumer = FakeConsumer()
    worker = EventWorker(KafkaSettings(), handler, consumer=consumer,
                         publisher=FakePublisher(), inbox=inbox)
    worker.handle_message(Msg(event))
    worker.handle_message(Msg(event, offset=9))
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM kafka_inbox").fetchone()[0] == 1
    assert len(consumer.commits) == 2


def test_failed_transaction_rolls_back_marker_and_business_effect(tmp_path):
    db = str(tmp_path / "rollback.db")
    inbox = SQLiteInbox(db, scope="orders")
    inbox.ensure_schema()
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE effects (id TEXT)")
    event = build_event("order.created", {"id": 1})

    def faulty(event, conn):
        conn.execute("INSERT INTO effects VALUES (?)", (event["event_id"],))
        raise OSError("database side-effect failed")

    with pytest.raises(OSError):
        inbox.process(event, faulty)
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM kafka_inbox").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 0
    assert inbox.process(event, lambda e, c: c.execute(
        "INSERT INTO effects VALUES (?)", (e["event_id"],))) is True
    assert inbox.process(event, lambda e, c: pytest.fail("duplicate executed")) is False


def sample_dlq(*, valid=True, original_topic="inhouse.events.v1"):
    original = build_event("order.created", {"id": 25})
    raw = json.dumps(original).encode() if valid else b"corrupt bytes"
    payload = {
        "original_topic": original_topic,
        "source_topic": original_topic + ".retry.2",
        "source_partition": 1,
        "source_offset": 33,
        "original_event_id": original["event_id"] if valid else None,
        "retry_attempt": 2,
        "error_type": "TimeoutError",
        "original_value_b64": base64.b64encode(raw).decode(),
        "original_key_b64": base64.b64encode(b"account-9").decode(),
        "original_key_was_null": False,
    }
    record = Msg(build_event("integration.delivery_failed", payload),
                 topic="inhouse.events.v1.dlq", partition=1, offset=44)
    return record, raw, original


def test_dlq_preview_and_confirmed_replay_preserve_event_id_and_key():
    record, raw, original = sample_dlq()
    publisher = FakePublisher()
    replay = DLQReplayer(KafkaSettings(), publisher=publisher)
    preview = replay.replay(record)
    assert preview["status"] == "dry-run"
    assert preview["replayable"] is True
    assert "payload" not in preview
    assert publisher.raw == []
    with pytest.raises(ValueError, match="confirm"):
        replay.replay(record, execute=True)
    result = replay.replay(record, execute=True, confirm_dlq_id=preview["dlq_event_id"])
    assert result["status"] == "published"
    topic, value, key, headers = publisher.raw[0]
    assert topic == "inhouse.events.v1"
    assert key == b"account-9"
    assert value == raw
    assert json.loads(value)["event_id"] == original["event_id"]
    assert b"inhouse.events.v1.dlq:1:44" == dict(headers)["ih-replayed-from-dlq"]


def test_dlq_poison_and_foreign_topic_cannot_be_replayed():
    publisher = FakePublisher()
    replay = DLQReplayer(KafkaSettings(), publisher=publisher)
    poison, _, _ = sample_dlq(valid=False)
    preview = replay.inspect(poison)
    assert preview["replayable"] is False
    with pytest.raises(ValueError, match="repair"):
        replay.replay(poison, execute=True, confirm_dlq_id=preview["dlq_event_id"])
    foreign, _, _ = sample_dlq(original_topic="unauthorized.topic")
    with pytest.raises(ValueError, match="Invalid or unsupported"):
        replay.inspect(foreign)
    assert publisher.raw == []


def test_topic_creation_includes_all_retry_stages():
    settings = topic_settings()
    future = type("Future", (), {"result": lambda self, timeout: None})()

    class Admin:
        def create_topics(self, specs, **kwargs):
            self.names = [spec.topic for spec in specs]
            return {name: future for name in self.names}

    admin = Admin()
    created = ensure_topics(settings, admin=admin)
    assert created == [settings.topic, settings.dlq_topic, *settings.retry_topics]


def test_retry_policy_rejects_invalid_configuration():
    with pytest.raises(ValueError, match="positive"):
        topic_settings(retry_delays_seconds=(-1.0,)).validate_retries()
    with pytest.raises(ValueError, match="retry"):
        KafkaSettings(retry_mode="unknown").validate_retries()
