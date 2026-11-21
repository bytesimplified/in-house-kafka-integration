-- Apply before using PostgresInbox, with your migration tool.
-- The business effect and marker must be written through one DB transaction.
CREATE TABLE IF NOT EXISTS kafka_inbox (
    consumer_scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_scope, event_id)
);
