"""Operator-controlled DLQ inspection and replay by exact Kafka partition/offset.

Never auto-commit or delete DLQ records. Replaying is explicitly at-least-once:
a repeat invocation may publish the same event twice. Preserve original event_id
so transactional inboxes can suppress already-processed effects.
"""
from __future__ import annotations

import base64
import binascii
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException, TopicPartition

from .producer import EventPublisher
from .settings import KafkaSettings


@dataclass(frozen=True)
class _Record:
    info: dict[str, Any]
    raw_value: bytes
    raw_key: bytes | None


class DLQReplayer:
    def __init__(
        self,
        settings: KafkaSettings,
        *,
        publisher: EventPublisher | None = None,
    ) -> None:
        self.settings = settings
        self.publisher = publisher if publisher is not None else EventPublisher(settings)

    def _parse(self, message: Any) -> _Record:
        if message.topic() != self.settings.dlq_topic:
            raise ValueError("Expected a DLQ record, not a source/retry record")
        try:
            envelope = json.loads((message.value() or b"").decode("utf-8"))
            if envelope["schema_version"] != 1 or envelope["event_type"] != "integration.delivery_failed":
                raise ValueError("Unexpected DLQ envelope type")
            payload = envelope["payload"]
            if not isinstance(payload, dict):
                raise ValueError("Invalid DLQ payload")
            destination = payload.get("original_topic", payload.get("source_topic"))
            if destination != self.settings.topic:
                raise ValueError("DLQ original topic is not the configured source topic")
            raw = base64.b64decode(payload["original_value_b64"], validate=True)
            key = base64.b64decode(payload["original_key_b64"], validate=True)
            raw_key = None if payload.get("original_key_was_null", False) else key
            original_id = payload.get("original_event_id")
            info = {
                "dlq_event_id": envelope["event_id"],
                "dlq_partition": message.partition(),
                "dlq_offset": message.offset(),
                "original_event_id": original_id,
                "destination_topic": destination,
                "failed_topic": payload["source_topic"],
                "failed_partition": payload["source_partition"],
                "failed_offset": payload["source_offset"],
                "retry_attempt": payload.get("retry_attempt", 0),
                "error_type": payload.get("error_type", "unknown"),
                "original_size_bytes": len(raw),
            }
        except (KeyError, TypeError, UnicodeError, binascii.Error, ValueError) as exc:
            raise ValueError("Invalid or unsupported DLQ record") from exc

        # Poison/malformed records need repair before replay. Do not repeatedly
        # publish bytes which the regular consumer will reject again.
        try:
            original = json.loads(raw.decode("utf-8"))
            valid = (
                isinstance(original, dict)
                and original.get("schema_version") == 1
                and isinstance(original.get("event_id"), str)
                and bool(original["event_id"])
                and isinstance(original.get("event_type"), str)
                and bool(original["event_type"])
                and isinstance(original.get("payload"), dict)
                and (original_id is None or original_id == original["event_id"])
            )
        except (ValueError, UnicodeError, TypeError):
            valid = False
        info["replayable"] = bool(valid)
        return _Record(info, raw, raw_key)

    def inspect(self, message: Any) -> dict[str, Any]:
        """Safe metadata-only preview; original payload/PII is not returned."""
        return dict(self._parse(message).info)

    def replay(
        self,
        message: Any,
        *,
        execute: bool = False,
        confirm_dlq_id: str | None = None,
    ) -> dict[str, Any]:
        item = self._parse(message)
        result = dict(item.info)
        if not execute:
            result["status"] = "dry-run"
            return result
        if not confirm_dlq_id or confirm_dlq_id != result["dlq_event_id"]:
            raise ValueError("Replay requires matching --confirm-dlq-id")
        if not result["replayable"]:
            raise ValueError("Malformed original event: repair upstream before replay")

        # No arbitrary target topic; preserve raw bytes, key and event_id.
        self.publisher.publish_raw(
            self.settings.topic,
            item.raw_value,
            key=item.raw_key,
            headers=[
                (
                    "ih-replayed-from-dlq",
                    f"{message.topic()}:{message.partition()}:{message.offset()}".encode("utf-8"),
                )
            ],
        )
        result["status"] = "published"
        return result

    def fetch(self, partition: int, offset: int, *, timeout_seconds: float = 15.0) -> Any:
        """Fetch an exact DLQ position without changing group offsets."""
        if partition < 0 or offset < 0 or timeout_seconds <= 0:
            raise ValueError("Invalid DLQ partition, offset or timeout")
        config = self.settings.consumer_config()
        config["group.id"] = f"{self.settings.group_id}.dlq-inspect.{uuid4()}"
        consumer = Consumer(config)
        try:
            consumer.assign([TopicPartition(self.settings.dlq_topic, partition, offset)])
            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
                if msg is None:
                    continue
                if msg.error() is not None:
                    raise KafkaException(msg.error())
                if msg.topic() != self.settings.dlq_topic or msg.partition() != partition:
                    raise RuntimeError("Unexpected partition read")
                if msg.offset() != offset:
                    raise LookupError("Requested DLQ offset is not available")
                return msg
            raise TimeoutError("Timed out reading the requested DLQ offset")
        finally:
            consumer.close()
