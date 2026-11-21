"""Stable versioned JSON event envelope."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def build_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    if not event_type or not isinstance(payload, dict):
        raise ValueError("An event_type and object payload are required")
    event = {
        "schema_version": 1,
        "event_id": str(uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    if correlation_id is not None:
        event["correlation_id"] = correlation_id
    return event
