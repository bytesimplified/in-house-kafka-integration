"""End-to-end broker smoke test, opt-in via KAFKA_INTEGRATION_TEST=1."""
import os
import time
from dataclasses import replace
from uuid import uuid4

import pytest

from inhouse_kafka.consumer import EventWorker
from inhouse_kafka.producer import EventPublisher
from inhouse_kafka.settings import KafkaSettings
from inhouse_kafka.topics import ensure_topics


@pytest.mark.integration
def test_publish_and_consume_via_kafka():
    if os.getenv("KAFKA_INTEGRATION_TEST") != "1":
        pytest.skip("Set KAFKA_INTEGRATION_TEST=1 after starting compose broker")

    settings = KafkaSettings.from_env()
    ensure_topics(settings)
    token = str(uuid4())
    group_settings = replace(settings, group_id="test-" + token)
    received = []
    worker = EventWorker(
        group_settings,
        lambda event: received.append(event) if event["payload"].get("marker") == token else None,
    )
    worker.consumer.subscribe([settings.topic])
    try:
        expected_id = EventPublisher(settings).publish(
            "integration.smoke_test", {"marker": token}, key=token
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not received:
            worker.poll_once(timeout=1.0)
        assert received, "Broker did not deliver event before deadline"
        assert received[0]["event_id"] == expected_id
    finally:
        worker.consumer.close()
