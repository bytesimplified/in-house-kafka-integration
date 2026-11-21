"""Transactional deduplication using an inbox table.

An event_id marker and business database changes MUST be committed in the
same transaction. Handlers receive (event, connection). External HTTP calls,
another database, or a different connection cannot be made exactly once by
this abstraction. Use an outbox for cross-system messaging.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Callable, Protocol


class TransactionalInbox(Protocol):
    def process(self, event: dict[str, Any], handler: Callable[..., None]) -> bool:
        """Return True if processed, False if already committed."""


def _require_scope(scope: str) -> None:
    if not scope or len(scope) > 200:
        raise ValueError("A stable non-empty consumer scope (<= 200 chars) is required")


class SQLiteInbox:
    """Durable, transactional inbox for local/single-host use (NOT shared across pods)."""

    def __init__(self, database_path: str | Path, *, scope: str) -> None:
        _require_scope(scope)
        self.database_path = str(database_path)
        self.scope = scope

    def ensure_schema(self) -> None:
        with sqlite3.connect(self.database_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kafka_inbox (
                    consumer_scope TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (consumer_scope, event_id)
                )
                """
            )

    def process(self, event: dict[str, Any], handler: Callable[..., None]) -> bool:
        event_id = event["event_id"]
        # BEGIN IMMEDIATE serializes concurrent writers and avoids a check/insert race.
        conn = sqlite3.connect(self.database_path, timeout=30, isolation_level=None)
        try:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                "INSERT OR IGNORE INTO kafka_inbox (consumer_scope, event_id) VALUES (?, ?)",
                (self.scope, event_id),
            )
            if cursor.rowcount == 0:
                conn.rollback()
                return False
            handler(event, conn)
            conn.commit()
            return True
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()


class PostgresInbox:
    """Multi-worker safe inbox. Business handler must write through SAME psycopg connection."""

    def __init__(
        self,
        dsn: str,
        *,
        scope: str,
        connection_factory: Callable[..., Any] | None = None,
    ) -> None:
        _require_scope(scope)
        self.dsn = dsn
        self.scope = scope
        self.connection_factory = connection_factory

    def _connect(self) -> Any:
        if self.connection_factory is not None:
            return self.connection_factory(self.dsn)
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("Install the postgres extra: pip install '.[postgres]'") from exc
        return psycopg.connect(self.dsn)

    def process(self, event: dict[str, Any], handler: Callable[..., None]) -> bool:
        # Unique constraint serializes competing consumers of the same ID.
        # The context commits on success and rolls back BOTH the marker and
        # business writes on failure.
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO kafka_inbox (consumer_scope, event_id)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                    RETURNING event_id
                    """,
                    (self.scope, event["event_id"]),
                )
                if cursor.fetchone() is None:
                    return False
            handler(event, conn)
            return True
