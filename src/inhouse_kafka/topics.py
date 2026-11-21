"""Explicit topic provisioning; application startup does not auto-create topics."""
from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .settings import KafkaSettings


def ensure_topics(settings: KafkaSettings, *, admin: Any = None) -> list[str]:
    if settings.partitions < 1 or settings.replication_factor < 1:
        raise ValueError("Topic partitions and replication factor must be positive")
    if settings.topic == settings.dlq_topic:
        raise ValueError("Event topic and DLQ topic must differ")
    client = admin if admin is not None else AdminClient(settings.connection_config())
    topic_names = [settings.topic, settings.dlq_topic]
    topic_specs = [
        NewTopic(
            name,
            num_partitions=settings.partitions,
            replication_factor=settings.replication_factor,
        )
        for name in topic_names
    ]
    results = client.create_topics(topic_specs, request_timeout=15)
    created = []
    for name in topic_names:
        try:
            results[name].result(timeout=20)
            created.append(name)
        except KafkaException as exc:
            if not exc.args or exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return created
