"""CLI for local provisioning, example publishing, and a demonstration consumer."""
from __future__ import annotations

import argparse
import json
import signal
from threading import Event

from .consumer import EventWorker
from .producer import EventPublisher
from .settings import KafkaSettings
from .topics import ensure_topics


def main() -> None:
    parser = argparse.ArgumentParser(description="In-house Kafka integration")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Provision event and DLQ topics")
    publish = commands.add_parser("produce", help="Publish one JSON object")
    publish.add_argument("--event-type", required=True)
    publish.add_argument("--payload", required=True, help='JSON object, e.g. {"id":1}')
    publish.add_argument("--key", help="Partitioning key, such as tenant ID")
    commands.add_parser("consume", help="Print events (replace with your business handler)")
    args = parser.parse_args()
    settings = KafkaSettings.from_env()

    if args.command == "init":
        created = ensure_topics(settings)
        print("Created topics:", ", ".join(created) if created else "(already exist)")
    elif args.command == "produce":
        payload = json.loads(args.payload)
        event_id = EventPublisher(settings).publish(args.event_type, payload, key=args.key)
        print("Published event_id:", event_id)
    else:
        stop = Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        worker = EventWorker(settings, lambda event: print(json.dumps(event), flush=True))
        worker.run_forever(should_stop=stop.is_set)


if __name__ == "__main__":
    main()
