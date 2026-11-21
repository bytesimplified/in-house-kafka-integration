"""Live broker and PostgreSQL integration coverage (opt-in)."""
from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from confluent_kafka import Consumer

from inhouse_kafka.consumer import EventWorker
from inhouse_kafka.events import build_event
from inhouse_kafka.inbox import PostgresInbox
from inhouse_kafka.producer import EventPublisher
from inhouse_kafka.replay import DLQReplayer
from inhouse_kafka.settings import KafkaSettings
from inhouse_kafka.topics import ensure_topics


def require_broker():
    if os.getenv("KAFKA_INTEGRATION_TEST") != "1":
        pytest.skip("Set KAFKA_INTEGRATION_TEST=1 after starting Kafka")


@pytest.mark.integration
def test_retry_topic_delayed_delivery_and_event_id_preservation():
    require_broker()
    token = str(uuid4())
    settings = replace(
        KafkaSettings.from_env(),
        group_id="retry-it-" + token,
        retry_mode="topics",
        retry_delays_seconds=(0.25, 0.5),
    )
    ensure_topics(settings)
    observed = []
    attempts = []

    def handler(event):
        if event["payload"].get("marker") != token:
            return
        attempts.append(event["event_id"])
        if len(attempts) == 1:
            raise OSError("temporary outage")
        observed.append(event["event_id"])

    worker = EventWorker(settings, handler)
    worker.consumer.subscribe([settings.topic, *settings.retry_topics], on_revoke=worker._on_revoke)
    try:
        event_id = EventPublisher(settings).publish(
            "integration.retry_case", {"marker": token}, key=token
        )
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and not observed:
            worker.poll_once(timeout=0.5)
        assert observed == [event_id]
        assert attempts == [event_id, event_id]
    finally:
        worker.consumer.close()


@pytest.mark.integration
def test_dlq_then_manually_replay_same_event_id():
    require_broker()
    token = str(uuid4())
    settings = replace(
        KafkaSettings.from_env(),
        group_id="dlq-it-" + token,
        retry_mode="topics",
        retry_delays_seconds=(0.15,),
    )
    ensure_topics(settings)
    state = {"allow": False, "success": []}

    def handler(event):
        if event["payload"].get("marker") != token:
            return
        if not state["allow"]:
            raise OSError("simulated dependency outage")
        state["success"].append(event["event_id"])

    worker = EventWorker(settings, handler)
    worker.consumer.subscribe([settings.topic, *settings.retry_topics], on_revoke=worker._on_revoke)
    config = settings.consumer_config()
    config["group.id"] = "dlq-observer-" + token
    dlq_consumer = Consumer(config)
    dlq_consumer.subscribe([settings.dlq_topic])
    try:
        published_id = EventPublisher(settings).publish(
            "integration.dlq_case", {"marker": token}, key=token
        )
        found = None
        deadline = time.monotonic() + 55
        while time.monotonic() < deadline and found is None:
            worker.poll_once(timeout=0.15)
            candidate = dlq_consumer.poll(timeout=0.15)
            if candidate is None or candidate.error():
                continue
            envelope = json.loads(candidate.value().decode())
            if envelope["payload"].get("original_event_id") == published_id:
                found = candidate
        assert found is not None, "Event was not routed to DLQ"
        replayer = DLQReplayer(settings)
        preview = replayer.inspect(found)
        assert preview["replayable"]
        assert preview["original_event_id"] == published_id
        assert replayer.replay(found)["status"] == "dry-run"
        state["allow"] = True
        result = replayer.replay(found, execute=True, confirm_dlq_id=preview["dlq_event_id"])
        assert result["status"] == "published"

        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and not state["success"]:
            worker.poll_once(timeout=0.5)
        assert state["success"] == [published_id]
    finally:
        dlq_consumer.close()
        worker.consumer.close()


@pytest.mark.integration
def test_postgres_inbox_concurrent_dedup_and_transaction_rollback():
    dsn = os.getenv("KAFKA_POSTGRES_DSN")
    if not dsn:
        pytest.skip("Set KAFKA_POSTGRES_DSN to run the shared-inbox test")
    import psycopg

    with psycopg.connect(dsn) as conn:
        conn.execute(Path("migrations/001_postgres_inbox.sql").read_text())
        conn.execute(
            "CREATE TABLE IF NOT EXISTS reliability_test_effects "
            "(event_id TEXT PRIMARY KEY, amount INTEGER NOT NULL)"
        )

    token = str(uuid4())
    inbox = PostgresInbox(dsn, scope="payments-" + token)
    event = build_event("payment.succeeded", {"amount": 25})

    def handle(event, conn):
        conn.execute(
            "INSERT INTO reliability_test_effects (event_id, amount) VALUES (%s, %s)",
            (event["event_id"], event["payload"]["amount"]),
        )

    # Concurrent clients compete on a unique inbox marker; only one effect commits.
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: inbox.process(event, handle), (1, 2)))
    assert sorted(results) == [False, True]
    with psycopg.connect(dsn) as conn:
        assert conn.execute(
            "SELECT count(*) FROM reliability_test_effects WHERE event_id=%s",
            (event["event_id"],),
        ).fetchone()[0] == 1

    failure_event = build_event("payment.succeeded", {"amount": 5})

    def abort(event, conn):
        handle(event, conn)
        raise RuntimeError("simulate rollback")

    with pytest.raises(RuntimeError):
        inbox.process(failure_event, abort)
    with psycopg.connect(dsn) as conn:
        assert conn.execute(
            "SELECT count(*) FROM kafka_inbox WHERE consumer_scope=%s AND event_id=%s",
            (inbox.scope, failure_event["event_id"]),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM reliability_test_effects WHERE event_id=%s",
            (failure_event["event_id"],),
        ).fetchone()[0] == 0
    assert inbox.process(failure_event, handle) is True
