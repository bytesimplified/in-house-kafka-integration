"""Synchronous, acknowledged Kafka publishing with idempotent producer semantics."""
from __future__ import annotations

import json
from typing import Any, Callable

from confluent_kafka import Producer

from .events import build_event
from .settings import KafkaSettings


class EventPublisher:
    def __init__(self, settings: KafkaSettings, *, producer: Any = None) -> None:
        self.settings = settings
        self._producer = producer if producer is not None else Producer(settings.producer_config())

    def publish(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        key: str | None = None,
        correlation_id: str | None = None,
        topic: str | None = None,
    ) -> str:
        """Publish and wait for broker delivery confirmation; raise on failure.

        Events may be delivered twice if callers retry following an ambiguous timeout.
        Consumers should deduplicate by event_id for exactly-once business effects.
        """
        event = build_event(event_type, payload, correlation_id=correlation_id)
        encoded = json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        delivery_errors: list[str] = []
        delivered: list[bool] = []

        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))
            else:
                delivered.append(True)

        self._producer.produce(
            topic=topic or self.settings.topic,
            key=key.encode("utf-8") if key is not None else None,
            value=encoded,
            on_delivery=on_delivery,
        )
        remaining = self._producer.flush(self.settings.delivery_timeout_seconds)
        if remaining != 0 or delivery_errors or not delivered:
            raise RuntimeError(
                "Kafka delivery not confirmed"
                + (": " + "; ".join(delivery_errors) if delivery_errors else "")
            )
        return event["event_id"]
