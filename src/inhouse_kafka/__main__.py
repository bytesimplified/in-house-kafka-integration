"""CLI for local topic administration, demo consumption and manual DLQ recovery."""
from __future__ import annotations

import argparse
import json
import signal
from threading import Event

from .consumer import EventWorker
from .producer import EventPublisher
from .replay import DLQReplayer
from .settings import KafkaSettings
from .topics import ensure_topics


def main() -> None:
    parser = argparse.ArgumentParser(description="In-house Kafka integration")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Provision source, DLQ and configured retry topics")
    publish = commands.add_parser("produce", help="Publish a JSON object")
    publish.add_argument("--event-type", required=True)
    publish.add_argument("--payload", required=True, help='JSON object, e.g. {"id":1}')
    publish.add_argument("--key", help="Partitioning key, e.g. tenant ID")
    commands.add_parser("consume", help="Print events (replace with your business handler)")
    replay = commands.add_parser("replay-dlq", help="Inspect or explicitly replay ONE DLQ offset")
    replay.add_argument("--partition", type=int, required=True)
    replay.add_argument("--offset", type=int, required=True)
    replay.add_argument("--execute", action="store_true", help="Actually publish; default is dry-run")
    replay.add_argument("--confirm-dlq-id", help="Exact DLQ envelope ID required when executing")
    args = parser.parse_args()
    if args.command == "replay-dlq" and args.execute and not args.confirm_dlq_id:
        parser.error("replay-dlq --execute requires --confirm-dlq-id")

    settings = KafkaSettings.from_env()
    if args.command == "init":
        created = ensure_topics(settings)
        print("Created topics:", ", ".join(created) if created else "(already exist)")
    elif args.command == "produce":
        payload = json.loads(args.payload)
        event_id = EventPublisher(settings).publish(args.event_type, payload, key=args.key)
        print("Published event_id:", event_id)
    elif args.command == "replay-dlq":
        replayer = DLQReplayer(settings)
        message = replayer.fetch(args.partition, args.offset)
        print(
            json.dumps(
                replayer.replay(
                    message, execute=args.execute, confirm_dlq_id=args.confirm_dlq_id,
                ),
                indent=2,
            )
        )
    else:
        stop = Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        worker = EventWorker(settings, lambda event: print(json.dumps(event), flush=True))
        worker.run_forever(should_stop=stop.is_set)


if __name__ == "__main__":
    main()
