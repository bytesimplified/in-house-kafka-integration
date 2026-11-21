"""Kafka configuration, including explicit retry-topic policy."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _retry_delays(value: str) -> tuple[float, ...]:
    try:
        delays = tuple(float(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise ValueError("KAFKA_RETRY_DELAYS_SECONDS must be comma-separated numbers") from exc
    return delays


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "inhouse-kafka"
    group_id: str = "inhouse-kafka-workers"
    topic: str = "inhouse.events.v1"
    dlq_topic: str = "inhouse.events.v1.dlq"
    partitions: int = 3
    replication_factor: int = 1
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_ca_location: str | None = None
    delivery_timeout_seconds: float = 10.0

    # Inline mode preserves the original API. Opt in to non-blocking source-topic
    # retries via KAFKA_RETRY_MODE=topics and provision retry topics first.
    retry_mode: str = "inline"
    retry_delays_seconds: tuple[float, ...] = (5.0, 30.0, 300.0)

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            client_id=os.getenv("KAFKA_CLIENT_ID", "inhouse-kafka"),
            group_id=os.getenv("KAFKA_GROUP_ID", "inhouse-kafka-workers"),
            topic=os.getenv("KAFKA_TOPIC", "inhouse.events.v1"),
            dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "inhouse.events.v1.dlq"),
            partitions=int(os.getenv("KAFKA_PARTITIONS", "3")),
            replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
            sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("KAFKA_SASL_USERNAME"),
            sasl_password=os.getenv("KAFKA_SASL_PASSWORD"),
            ssl_ca_location=os.getenv("KAFKA_SSL_CA_LOCATION"),
            retry_mode=os.getenv("KAFKA_RETRY_MODE", "inline").lower(),
            retry_delays_seconds=_retry_delays(os.getenv("KAFKA_RETRY_DELAYS_SECONDS", "5,30,300")),
        )

    @property
    def retry_topics(self) -> tuple[str, ...]:
        return tuple(f"{self.topic}.retry.{i}" for i in range(1, len(self.retry_delays_seconds) + 1))

    def validate_retries(self) -> None:
        if self.retry_mode not in ("inline", "topics"):
            raise ValueError("KAFKA_RETRY_MODE must be inline or topics")
        if len(self.retry_delays_seconds) > 20 or any(
            delay <= 0 or delay > 7 * 24 * 3600 or delay != delay
            for delay in self.retry_delays_seconds
        ):
            raise ValueError("Retry delays must be finite, positive and <= 7 days (max 20 stages)")
        if not self.topic or not self.dlq_topic:
            raise ValueError("Topic names cannot be empty")
        topic_names = [self.topic, self.dlq_topic, *self.retry_topics]
        if len(set(topic_names)) != len(topic_names):
            raise ValueError("Event, retry, and DLQ topic names must differ")

    def connection_config(self) -> dict[str, object]:
        if self.security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("Unsupported KAFKA_SECURITY_PROTOCOL")
        config: dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL_"):
            if not all((self.sasl_mechanism, self.sasl_username, self.sasl_password)):
                raise ValueError("SASL requires mechanism, username and password")
            config.update({
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
            })
        if self.ssl_ca_location and self.security_protocol in {"SSL", "SASL_SSL"}:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config

    def producer_config(self) -> dict[str, object]:
        return {
            **self.connection_config(),
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": int(self.delivery_timeout_seconds * 1000),
        }

    def consumer_config(self) -> dict[str, object]:
        return {
            **self.connection_config(),
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
