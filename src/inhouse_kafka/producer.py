"""Acknowledged publishing; raw forwarding keeps the original event_id intact."""
from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Producer

from .events import build_event
from .settings import KafkaSettings


class EventPublisher:
    def __init__(self, settings: KafkaSettings, *, producer: Any = None) -> None:
        self.settings = settings
        self._producer = producer if producer is not None else Producer(settings.producer_config())

    def publish_raw(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        """Require broker acknowledgement before callers commit a consumed offset.

        Publishing and consuming are NOT in one Kafka transaction. On an
        ambiguous timeout or crash between acknowledgement and source commit,
        the forwarded record may be duplicated. Consumers require deduplication.
        """
        failures: list[str] = []
        delivered: list[bool] = []

        def on_delivery(error: Any, message: Any) -> None:
            if error is not None:
                failures.append(str(error))
            else:
                delivered.append(True)

        kwargs: dict[str, Any] = {"topic": topic, "key": key, "value": value, "on_delivery": on_delivery}
        if headers is not None:
            kwargs["headers"] = headers
        self._producer.produce(**kwargs)
        remaining = self._producer.flush(self.settings.delivery_timeout_seconds)
        if remaining or failures or not delivered:
            raise RuntimeError(
                "Kafka delivery not confirmed" + (": " + "; ".join(failures) if failures else "")
            )

    def publish(
        self,
        event_type: str,
        payload: dict[str, Any],
        *,
        key: str | None = None,
        correlation_id: str | None = None,
        topic: str | None = None,
    ) -> str:
        event = build_event(event_type, payload, correlation_id=correlation_id)
        self.publish_raw(
            topic or self.settings.topic,
            json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            key=key.encode("utf-8") if key is not None else None,
        )
        return event["event_id"]
