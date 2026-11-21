"""At-least-once consumer with bounded retries and a dead-letter topic."""
from __future__ import annotations

import base64
import json
import time
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaException

from .producer import EventPublisher
from .settings import KafkaSettings


class EventWorker:
    def __init__(
        self,
        settings: KafkaSettings,
        handler: Callable[[dict[str, Any]], None],
        *,
        consumer: Any = None,
        publisher: EventPublisher | None = None,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.5,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1 or retry_backoff_seconds < 0:
            raise ValueError("max_attempts must be positive and backoff nonnegative")
        self.settings = settings
        self.handler = handler
        self.consumer = consumer if consumer is not None else Consumer(settings.consumer_config())
        self.publisher = publisher if publisher is not None else EventPublisher(settings)
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleeper = sleeper

    def _send_to_dlq(self, message: Any, error: Exception) -> None:
        original_value = message.value() or b""
        original_key = message.key() or b""
        self.publisher.publish(
            "integration.delivery_failed",
            {
                "source_topic": message.topic(),
                "source_partition": message.partition(),
                "source_offset": message.offset(),
                "error_type": type(error).__name__,
                "error_message": str(error)[:256],
                "original_value_b64": base64.b64encode(original_value).decode("ascii"),
                "original_key_b64": base64.b64encode(original_key).decode("ascii"),
            },
            topic=self.settings.dlq_topic,
        )

    def handle_message(self, message: Any) -> None:
        try:
            data = json.loads(message.value().decode("utf-8"))
            if not isinstance(data, dict) or data.get("schema_version") != 1:
                raise ValueError("Expected a version 1 JSON event envelope")
            if not isinstance(data.get("event_id"), str) or not isinstance(data.get("payload"), dict):
                raise ValueError("Missing event_id or object payload")
        except (ValueError, UnicodeError, AttributeError) as exc:
            self._send_to_dlq(message, exc)
            self.consumer.commit(message=message, asynchronous=False)
            return

        for attempt in range(self.max_attempts):
            try:
                self.handler(data)
                break
            except Exception as exc:
                if attempt + 1 == self.max_attempts:
                    self._send_to_dlq(message, exc)
                    self.consumer.commit(message=message, asynchronous=False)
                    return
                self.sleeper(self.retry_backoff_seconds * (2 ** attempt))

        # A handler returning normally has completed its side effects.
        # Commit after processing. Reprocessing after a crash is still possible.
        self.consumer.commit(message=message, asynchronous=False)

    def poll_once(self, timeout: float = 1.0) -> bool:
        """Poll one record. Return True if a record was handled."""
        message = self.consumer.poll(timeout)
        if message is None:
            return False
        if message.error() is not None:
            raise KafkaException(message.error())
        self.handle_message(message)
        return True

    def run_forever(self, *, should_stop: Callable[[], bool] = lambda: False) -> None:
        self.consumer.subscribe([self.settings.topic])
        try:
            while not should_stop():
                self.poll_once()
        finally:
            self.consumer.close()
