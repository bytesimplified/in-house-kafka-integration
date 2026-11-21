"""At-least-once consumer with optional delayed retry topics and transactional inbox.

In topic mode a failed record is forwarded before committing its source offset.
Kafka transactions do not span that forwarding and commit; duplicates can arise.
Use an inbox transaction for exactly-once database side effects.
"""
from __future__ import annotations

import base64
import json
import logging
import time
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaException, TopicPartition

from .producer import EventPublisher
from .settings import KafkaSettings

log = logging.getLogger(__name__)
_RETRY_ATTEMPT = "ih-retry-attempt"
_NOT_BEFORE = "ih-not-before-ms"
_ORIGINAL_TOPIC = "ih-original-topic"
_CONTROL = {_RETRY_ATTEMPT, _NOT_BEFORE, _ORIGINAL_TOPIC}


class PermanentProcessingError(Exception):
    """Non-retryable business error (invalid state, rejected request, etc.)."""


class EventWorker:
    def __init__(
        self,
        settings: KafkaSettings,
        handler: Callable[..., None],
        *,
        consumer: Any = None,
        publisher: EventPublisher | None = None,
        inbox: Any = None,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 0.5,
        sleeper: Callable[[float], None] = time.sleep,
        clock_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ) -> None:
        settings.validate_retries()
        if max_attempts < 1 or retry_backoff_seconds < 0:
            raise ValueError("max_attempts must be positive and backoff nonnegative")
        self.settings = settings
        self.handler = handler
        self.consumer = consumer if consumer is not None else Consumer(settings.consumer_config())
        self.publisher = publisher if publisher is not None else EventPublisher(settings)
        self.inbox = inbox
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.sleeper = sleeper
        self.clock_ms = clock_ms
        # Paused retry partitions; hold messages in memory while regularly
        # polling other partitions to maintain membership/heartbeats.
        self._pending: dict[tuple[str, int], tuple[int, Any]] = {}

    @staticmethod
    def _headers(message: Any) -> list[tuple[str, bytes]]:
        getter = getattr(message, "headers", None)
        return list(getter() or []) if getter is not None else []

    def _retry_metadata(self, message: Any) -> tuple[int, int]:
        values = dict(self._headers(message))
        try:
            attempt = int((values.get(_RETRY_ATTEMPT) or b"0").decode("ascii"))
            due_ms = int((values.get(_NOT_BEFORE) or b"0").decode("ascii"))
        except (ValueError, UnicodeError, AttributeError) as exc:
            raise PermanentProcessingError("Invalid retry headers") from exc

        if attempt < 0 or due_ms < 0:
            raise PermanentProcessingError("Invalid negative retry metadata")
        if self.settings.retry_mode == "topics":
            expected = (
                self.settings.retry_topics.index(message.topic()) + 1
                if message.topic() in self.settings.retry_topics
                else 0
            )
            if message.topic() != self.settings.topic and message.topic() not in self.settings.retry_topics:
                raise PermanentProcessingError("Unknown source topic")
            if attempt != expected:
                raise PermanentProcessingError("Retry attempt does not match topic stage")
        return attempt, due_ms

    def _decode(self, message: Any) -> dict[str, Any]:
        try:
            value = json.loads((message.value() or b"").decode("utf-8"))
        except (ValueError, UnicodeError, AttributeError) as exc:
            raise PermanentProcessingError("Invalid UTF-8 JSON") from exc
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != 1
            or not isinstance(value.get("event_id"), str)
            or not value["event_id"]
            or not isinstance(value.get("event_type"), str)
            or not value["event_type"]
            or not isinstance(value.get("payload"), dict)
        ):
            raise PermanentProcessingError("Expected a version 1 JSON event envelope")
        return value

    def _send_to_dlq(
        self, message: Any, error: Exception, *, attempt: int = 0, event_id: str | None = None
    ) -> None:
        # The raw bytes make manual replay lossless; base64 is NOT encryption.
        original_value = message.value() or b""
        original_key = message.key() or b""
        self.publisher.publish(
            "integration.delivery_failed",
            {
                "original_topic": self.settings.topic,
                "source_topic": message.topic(),
                "source_partition": message.partition(),
                "source_offset": message.offset(),
                "original_event_id": event_id,
                "retry_attempt": attempt,
                "error_type": type(error).__name__,
                "error_message": str(error)[:256],
                "original_value_b64": base64.b64encode(original_value).decode("ascii"),
                "original_key_b64": base64.b64encode(original_key).decode("ascii"),
                "original_key_was_null": message.key() is None,
            },
            topic=self.settings.dlq_topic,
        )
        log.warning(
            "Sent message to DLQ topic=%s partition=%s offset=%s attempt=%s",
            message.topic(), message.partition(), message.offset(), attempt,
        )

    def _forward_retry(self, message: Any, *, attempt: int) -> None:
        delay = self.settings.retry_delays_seconds[attempt]
        next_topic = self.settings.retry_topics[attempt]
        headers = [(key, value) for key, value in self._headers(message) if key not in _CONTROL]
        headers.extend([
            (_RETRY_ATTEMPT, str(attempt + 1).encode("ascii")),
            (_NOT_BEFORE, str(self.clock_ms() + int(delay * 1000)).encode("ascii")),
            (_ORIGINAL_TOPIC, self.settings.topic.encode("utf-8")),
        ])
        self.publisher.publish_raw(
            next_topic,
            message.value() or b"",
            key=message.key(),
            headers=headers,
        )
        log.info(
            "Scheduled retry topic=%s source=%s partition=%s offset=%s attempt=%s",
            next_topic, message.topic(), message.partition(), message.offset(), attempt + 1,
        )

    def _process(self, event: dict[str, Any]) -> None:
        if self.inbox is None:
            self.handler(event)
        else:
            # Transactional handlers must accept (event, db_connection) and
            # write their business state using that SAME connection.
            self.inbox.process(event, self.handler)

    def handle_message(self, message: Any) -> None:
        """Handle and sync-commit one record, or raise so the worker stops without committing."""
        try:
            attempt, _ = self._retry_metadata(message)
            event = self._decode(message)
        except PermanentProcessingError as exc:
            self._send_to_dlq(message, exc)
            self.consumer.commit(message=message, asynchronous=False)
            return

        if self.settings.retry_mode == "topics":
            try:
                self._process(event)
            except PermanentProcessingError as exc:
                self._send_to_dlq(message, exc, attempt=attempt, event_id=event["event_id"])
            except Exception as exc:
                if attempt < len(self.settings.retry_delays_seconds):
                    self._forward_retry(message, attempt=attempt)
                else:
                    self._send_to_dlq(message, exc, attempt=attempt, event_id=event["event_id"])
            # If forwarding or the DLQ publish fails, this line is never reached.
            self.consumer.commit(message=message, asynchronous=False)
            return

        # Backwards-compatible inline mode.
        for index in range(self.max_attempts):
            try:
                self._process(event)
                break
            except PermanentProcessingError as exc:
                self._send_to_dlq(message, exc, event_id=event["event_id"])
                break
            except Exception as exc:
                if index + 1 == self.max_attempts:
                    self._send_to_dlq(message, exc, attempt=index, event_id=event["event_id"])
                    break
                self.sleeper(self.retry_backoff_seconds * (2 ** index))
        self.consumer.commit(message=message, asynchronous=False)

    def _process_due(self) -> bool:
        now = self.clock_ms()
        for identity, (due_ms, message) in list(self._pending.items()):
            if due_ms <= now:
                # Do not remove the pending record if processing or commit fails.
                self.handle_message(message)
                self._pending.pop(identity, None)
                self.consumer.resume([TopicPartition(*identity)])
                return True
        return False

    def poll_once(self, timeout: float = 1.0) -> bool:
        if self._process_due():
            return True
        if self._pending:
            next_due = min(due for due, _ in self._pending.values())
            timeout = max(0.0, min(timeout, (next_due - self.clock_ms()) / 1000.0))
        message = self.consumer.poll(timeout)
        if message is None:
            return False
        if message.error() is not None:
            raise KafkaException(message.error())

        if self.settings.retry_mode == "topics" and message.topic() in self.settings.retry_topics:
            try:
                _, due_ms = self._retry_metadata(message)
            except PermanentProcessingError:
                # A malformed retry record is handled as poison by handle_message.
                self.handle_message(message)
                return True
            if due_ms > self.clock_ms():
                identity = (message.topic(), message.partition())
                if identity in self._pending:
                    raise RuntimeError("Received another record from a paused retry partition")
                self.consumer.pause([TopicPartition(*identity)])
                self._pending[identity] = (due_ms, message)
                return False
        self.handle_message(message)
        return True

    def _on_revoke(self, consumer: Any, partitions: Any) -> None:
        # The offsets were not committed, so the new owner will consume again.
        revoked = {(p.topic, p.partition) for p in partitions}
        for identity in revoked:
            self._pending.pop(identity, None)

    def run_forever(self, *, should_stop: Callable[[], bool] = lambda: False) -> None:
        topics = [self.settings.topic]
        if self.settings.retry_mode == "topics":
            topics.extend(self.settings.retry_topics)
        self.consumer.subscribe(topics, on_revoke=self._on_revoke)
        try:
            while not should_stop():
                self.poll_once()
        finally:
            self.consumer.close()
