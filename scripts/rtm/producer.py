#!/usr/bin/env python3
"""
Kafka producer for RTM demo pipelines.

Produces JSON events with embedded timestamps to the rtm-input topic.
Events simulate user interactions (clicks, purchases, views).

Usage:
    python producer.py [--rate 1000] [--keys 100] [--bootstrap-servers localhost:9092]
                       [--dup-rate 0.02]

Copied from spark-rtm-quickstart (the Spark 4.1 RTM quickstart) so latency
numbers stay comparable. Only change: --dup-rate re-sends that fraction of
events right after the original with the same event_id (like the
ghost-kitchen chaos generator), for the dropDuplicates experiment.
Duplicates are sent on top of --rate.
"""

import argparse
import json
import random
import signal
import time
import uuid

from kafka import KafkaProducer

EVENT_TYPES = ["click", "purchase", "view"]
EVENT_WEIGHTS = [0.5, 0.1, 0.4]  # clicks most common, purchases rare


def create_event(num_keys: int) -> dict:
    """Generate a single event with an embedded timestamp."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        "key": f"user_{random.randint(1, num_keys)}",
        "value": random.randint(1, 1000),
        "produced_at": int(time.time() * 1000),  # epoch milliseconds
    }


def main():
    parser = argparse.ArgumentParser(description="RTM demo Kafka producer")
    parser.add_argument(
        "--rate", type=int, default=1000, help="Events per second (default: 1000)"
    )
    parser.add_argument(
        "--keys",
        type=int,
        default=100,
        help="Number of unique user keys (default: 100)",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="rtm-input",
        help="Kafka topic to produce to (default: rtm-input)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--dup-rate",
        type=float,
        default=0.0,
        help="Fraction of events re-sent as duplicates (default: 0)",
    )
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=5,
        batch_size=16384,
    )

    # Graceful shutdown
    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    sent = 0
    dups = 0
    start_time = time.time()

    print(
        f"Producing {args.rate} events/sec to {args.topic} "
        f"({args.keys} keys, bootstrap: {args.bootstrap_servers})"
    )
    print("Press Ctrl+C to stop.\n")

    try:
        while running:
            batch_start = time.time()
            # Send a batch of events for this second
            for _ in range(args.rate):
                if not running:
                    break
                event = create_event(args.keys)
                producer.send(args.topic, key=event["key"], value=event)
                sent += 1
                if args.dup_rate and random.random() < args.dup_rate:
                    producer.send(args.topic, key=event["key"], value=event)
                    sent += 1
                    dups += 1

            # Flush and report
            producer.flush()
            elapsed = time.time() - start_time
            actual_rate = sent / elapsed if elapsed > 0 else 0
            print(
                f"  Sent {sent:,} events | Elapsed: {elapsed:.1f}s | "
                f"Rate: {actual_rate:,.0f} events/sec"
            )

            # Sleep to maintain target rate
            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        print(
            f"\nDone. Sent {sent:,} events ({dups:,} duplicates) in {elapsed:.1f}s "
            f"({sent / elapsed:,.0f} events/sec)"
        )


if __name__ == "__main__":
    main()
