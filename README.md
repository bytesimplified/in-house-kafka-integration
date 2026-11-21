# In-house Kafka integration infrastructure

A standalone Python reference implementation of an internal Kafka event integration, in its dedicated repository. It provides a versioned JSON envelope, acknowledged/idempotent producer, at-least-once consumer with bounded retries, dead-letter topic (DLQ), explicit topic provisioning, and optional SASL/TLS client configuration.

## Architecture

```text
Application -> EventPublisher -> inhouse.events.v1 -> EventWorker -> business handler
                                             | retry 3x + exponential backoff
                                             +-> inhouse.events.v1.dlq (terminal failures)
```

Every processed record is committed **only after** the handler succeeds or a DLQ write is broker-acknowledged. A failed DLQ write prevents the source offset from being committed. This is **at-least-once**, not exactly-once business processing: a crash after a handler succeeds but before offset commit can result in duplicate processing. Use `event_id` plus a transactional inbox/deduplication store for downstream side effects. The producer's idempotence prevents duplicates from its own internal retries, not arbitrary application replays.

## Local start

Prerequisites: Docker Compose, Python 3.11+.

```bash
git clone https://github.com/bytesimplified/in-house-kafka-integration.git
cd in-house-kafka-integration
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
docker compose up -d --wait
python -m inhouse_kafka init
python -m inhouse_kafka consume
```

From a second shell with the same virtual environment:

```bash
python -m inhouse_kafka produce --event-type order.created --key tenant-42 --payload '{"order_id":"ord-1","amount":42}'
```

Run tests:

```bash
pytest -m "not integration"
KAFKA_INTEGRATION_TEST=1 pytest -m integration
docker compose down -v
```

## Integrating into a service

```python
from inhouse_kafka import EventPublisher, EventWorker, KafkaSettings

config = KafkaSettings.from_env()
publisher = EventPublisher(config)
event_id = publisher.publish("order.created", {"order_id": "ord-1"}, key="tenant-42")

def process(event: dict) -> None:
    # Validate expected event_type, then perform idempotent business side effects.
    # Raise on transient/permanent errors so the worker retries and eventually DLQs.
    print(event["event_id"], event["payload"])

EventWorker(config, process).run_forever()
```

The example consumer prints events for demonstration; replace the handler before deployment. Keep a stable key (e.g., tenant/account ID) when partition ordering matters. Consumers in the same `KAFKA_GROUP_ID` load-share partitions; distinct groups each receive their own copy.

## Configuration

See `.env.example`. The application reads **environment variables**, not the .env file directly. Supply them from your runtime's environment or secret manager. Key settings include:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Comma-separated bootstrap brokers |
| `KAFKA_CLIENT_ID` | inhouse-kafka | Client identifier |
| `KAFKA_GROUP_ID` | inhouse-kafka-workers | Consumer group |
| `KAFKA_TOPIC` | inhouse.events.v1 | Source topic |
| `KAFKA_DLQ_TOPIC` | inhouse.events.v1.dlq | Dead-letter topic |
| `KAFKA_PARTITIONS` | 3 | Provisioned partitions |
| `KAFKA_REPLICATION_FACTOR` | 1 | Provisioned replication factor |
| `KAFKA_SECURITY_PROTOCOL` | PLAINTEXT | Set SSL or SASL_SSL in production |
| `KAFKA_SASL_MECHANISM` | unset | e.g. SCRAM-SHA-512 |
| `KAFKA_SASL_USERNAME` | unset | Service identity |
| `KAFKA_SASL_PASSWORD` | unset | Inject securely |
| `KAFKA_SSL_CA_LOCATION` | unset | Custom broker CA file |

`init` is an explicit administrative step requiring topic-create privileges. Applications need produce/consume privileges only. Provision topics as managed infrastructure for real deployments rather than giving all services admin credentials.

## Production considerations

- **Not a production cluster:** Compose runs one PLAINTEXT broker bound to loopback. For production use a managed/high-availability Kafka cluster (3+ brokers, replication factor >=3, minimum in-sync replicas >=2) and TLS/SASL with per-service ACLs.
- **Security and privacy:** Do not put secrets into events. The DLQ stores the original payload in base64 (not encryption), so apply the same classification, ACLs, retention and access controls as the source topic. Enable encryption at rest where supported.
- **Reliability:** Use an outbox pattern if database writes and event publication must be atomic. The default three retries are inline and **block the partition** during backoff; for lengthy failures use separate retry topics and an operational DLQ replay strategy.
- **Operations:** Configure retention, quotas, alarms on consumer lag and DLQ growth, schema compatibility enforcement, service metrics/tracing and replay ownership before production rollout. This starter does not include a Schema Registry, consumer-side dedup store, telemetry exporter, or Kubernetes/Terraform provisioning.
- **Evolving contracts:** Bump `schema_version` and create versioned topic contracts for breaking event changes; avoid sending unvalidated arbitrary payloads from untrusted producers.
