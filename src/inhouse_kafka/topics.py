"""Explicit topic provisioning; never rely on Kafka auto-creation."""
from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .settings import KafkaSettings


def ensure_topics(settings: KafkaSettings, *, admin: Any = None) -> list[str]:
    settings.validate_retries()
    if settings.partitions < 1 or settings.replication_factor < 1:
        raise ValueError("Topic partitions and replication factor must be positive")
    client = admin if admin is not None else AdminClient(settings.connection_config())
    topic_names = [settings.topic, settings.dlq_topic]
    if settings.retry_mode == "topics":
        topic_names.extend(settings.retry_topics)
    specs = [
        NewTopic(name, num_partitions=settings.partitions, replication_factor=settings.replication_factor)
        for name in topic_names
    ]
    results = client.create_topics(specs, request_timeout=15)
    created = []
    for name in topic_names:
        try:
            results[name].result(timeout=20)
            created.append(name)
        except KafkaException as exc:
            if not exc.args or exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return created
