# In-house Kafka integration infrastructure

An in-house event integration library with acknowledged Kafka publishing, at-least-once consumption, **staged delayed retry topics**, **transactional inbox deduplication**, and **operator-controlled dead-letter queue (DLQ) recovery**.

This is application-side reliability code, **not a production Kafka cluster**. Docker Compose runs one plaintext broker and an optional development PostgreSQL instance.

## Delivery architecture

~~~text
App -> source topic -> EventWorker -> transactional business handler
                          |
                          +--> retry.1 (5s) --> retry.2 (30s) --> retry.3 (300s)
                          |                                           |
                          +--> malformed / permanent / exhausted ----+--> DLQ
                                                                         |
                                                  preview + confirm --> replay to source
~~~

- A producer uses Kafka idempotence and acks=all and waits for delivery confirmation.
- In **topics** mode, the worker forwards the **original event bytes and key**, including the same event_id, to the next retry stage. A consumed source/retry offset is synchronously committed only after the next stage or DLQ write is broker-acknowledged.
- Retry-topic headers carry the attempt number and due time. Not-yet-due retry partitions are paused while other partitions continue polling. A partition is resumed after the held record is processed; rebalance revocation abandons uncommitted local pending work for the new owner to retry.
- PermanentProcessingError and invalid events bypass retries to the DLQ. Other failures traverse configured retry stages before DLQ.
- PostgreSQL inbox deduplication commits a unique (consumer_scope, event_id) marker with the business changes made using **the same database connection and transaction**. SQLiteInbox is offered for single-host development.

**Guarantee:** Kafka forwarding plus source offset commit are NOT an atomic Kafka transaction. A crash after forwarding can duplicate retry/DLQ records. PostgresInbox protects database effects only if the handler performs them on the provided connection. Calls to an HTTP API, email, or a different database are NOT exactly-once; use an outbox or downstream idempotency keys. Routing to retries can reorder events relative to later source messages.

## Local quickstart

Prerequisites: Docker Compose and Python 3.11+. The app reads **exported environment variables**, not dotenv files.

~~~bash
git clone https://github.com/bytesimplified/in-house-kafka-integration.git
cd in-house-kafka-integration
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test,postgres]'

export KAFKA_RETRY_MODE=topics
export KAFKA_RETRY_DELAYS_SECONDS=5,30,300
docker compose --profile inbox up -d --wait
python -m inhouse_kafka init
python -m inhouse_kafka consume
~~~

In a second shell using the same Python environment:

~~~bash
python -m inhouse_kafka produce \
  --event-type order.created --key tenant-42 \
  --payload '{"order_id":"ord-1","amount":42}'
~~~

The built-in consumer prints events **only for demonstration**. Replace with your business handler. The default mode remains **inline** with three in-process attempts for backwards compatibility. For staged non-blocking source-topic retries, set KAFKA_RETRY_MODE=topics and provision topics before starting workers.

### Transactional PostgreSQL inbox

Apply the migration to the database used by the business handler:

~~~bash
docker compose exec -T postgres psql -U kafka -d kafka < migrations/001_postgres_inbox.sql
export KAFKA_INBOX_DSN='postgresql://kafka:local-dev-only@localhost:5432/kafka'
~~~

Example handler. The application-owned orders table must already exist in that same DB:

~~~python
import os
from inhouse_kafka import EventWorker, KafkaSettings, PostgresInbox

settings = KafkaSettings.from_env()
inbox = PostgresInbox(os.environ["KAFKA_INBOX_DSN"], scope=settings.group_id)

def process(event: dict, conn) -> None:
    if event["event_type"] != "order.created":
        return
    conn.execute(
        "UPDATE orders SET state = 'received' WHERE order_id = %s",
        (event["payload"]["order_id"],),
    )

EventWorker(settings, process, inbox=inbox).run_forever()
~~~

Handler-side writes and the inbox marker must share the SAME connection. Choose a stable consumer scope (usually group/service name), and use a shared PostgreSQL database for multiple replicas. SQLiteInbox(path, scope=...) with ensure_schema() is suitable only for single-host/local use.

## Inspect and replay a DLQ message safely

Locate the DLQ message's partition/offset with Kafka tooling, e.g.:

~~~bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic inhouse.events.v1.dlq --from-beginning \
  --property print.partition=true --property print.offset=true
~~~

First preview **without publishing or exposing original payload contents**:

~~~bash
python -m inhouse_kafka replay-dlq --partition 0 --offset 15
~~~

The preview includes DLQ envelope ID, original event ID, failure classification, destination, and replayability. After resolving the failure, confirm that exact DLQ event ID:

~~~bash
python -m inhouse_kafka replay-dlq \
  --partition 0 --offset 15 \
  --execute --confirm-dlq-id "dlq-id-from-preview"
~~~

Safeguards: no replay by default; no arbitrary destination topic (only the configured source); exact confirmation required; preserve original event bytes/key/event_id; refuse malformed/poison events pending repair; no commit or removal of DLQ offsets. Repeating --execute **will republish**; record replay operations and use downstream deduplication. Restrict DLQ reads and replay permissions, because base64 encoding is NOT encryption.

## Failure handling runbook

| Event | Handling |
| --- | --- |
| Transient handler exception | Publish to next retry topic and commit original only after ack |
| PermanentProcessingError | Direct DLQ |
| Invalid JSON, wrong schema or mismatched retry stage | Direct DLQ |
| Retry/DLQ broker publish failure | Raise, stop worker and leave record uncommitted |
| Source offset commit failure | Redelivery is possible; transactional inbox suppresses repeated DB effects |
| Rebalance with delayed retry | Drop local pending record; new partition owner re-reads from committed offset |
| DLQ replay | Metadata-only dry-run, then explicit confirmation and publication |

## Configuration

See .env.example. Important runtime variables:

| Setting | Default | Description |
| --- | --- | --- |
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Kafka endpoints |
| KAFKA_GROUP_ID | inhouse-kafka-workers | Consumer group |
| KAFKA_TOPIC | inhouse.events.v1 | Primary topic |
| KAFKA_DLQ_TOPIC | inhouse.events.v1.dlq | Dead-letter topic |
| KAFKA_RETRY_MODE | inline | inline or topics |
| KAFKA_RETRY_DELAYS_SECONDS | 5,30,300 | One stage per duration, in seconds |
| KAFKA_PARTITIONS | 3 | Topic partitions |
| KAFKA_REPLICATION_FACTOR | 1 | Local replication factor |
| KAFKA_SECURITY_PROTOCOL | PLAINTEXT | Use SSL or SASL_SSL in production |
| KAFKA_INBOX_DSN | unset | Application setting for optional PostgresInbox |

The topic names are source-topic.retry.1, source-topic.retry.2, etc. Topic provisioning is explicit via the init CLI; service accounts need produce/consume ACLs only. Treat changes to retry stage count/naming as migrations when existing retry records may still be in flight.

## Test suite

~~~bash
pytest -m "not integration" -q
docker compose --profile inbox up -d --wait
KAFKA_INTEGRATION_TEST=1 \
KAFKA_POSTGRES_DSN='postgresql://kafka:local-dev-only@localhost:5432/kafka' \
  pytest -m integration -q
docker compose --profile inbox down -v
~~~

GitHub Actions runs unit tests, live Kafka forwarding/replay tests and a concurrent PostgreSQL deduplication/rollback test.

## Production rollout requirements

1. Use a managed or multi-broker Kafka cluster, replication/min-insync-replicas that meet your SLO, encrypted connections, SASL and least-privilege topic ACLs. Compose is only for local development.
2. Monitor source/retry consumer lag, retry attempts, DLQ growth, broker delivery failures, worker restarts and paused retry partitions. Retry-topic retention must exceed the longest configured delay plus your expected recovery window.
3. Adopt a schema registry and event contracts, classify permanent errors, secure sensitive payloads, and protect DLQ contents as strictly as source topics.
4. Use PostgresInbox only when all effects are in the same DB transaction; add an outbox or downstream idempotency for other systems. Keep replay approvals/audit records, as the CLI has no replay ledger and repeated replays can duplicate.
5. Retry partition pausing avoids blocking the source partition, but it **does** delay later records in the same retry partition. Test latency, rebalances, failure injection and recovery under expected load before rollout.
6. Ensure database migrations, backup/restore, incident runbooks, dashboards, controlled release processes and operator access controls are in place.

**Do not interpret at-least-once Kafka processing as end-to-end exactly-once across external systems.**
