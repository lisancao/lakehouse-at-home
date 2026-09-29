"""Kafka streaming producer with speed multiplier replay."""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer

from .config import GeneratorConfig, ChaosConfig
from .chaos import ChaosMonkey


class StreamingProducer:
    """Replays events from parquet to Kafka with configurable speed."""

    def __init__(
        self,
        config: GeneratorConfig,
        speed_multiplier: Optional[int] = None,
        start_day: int = 0,
    ):
        self.config = config
        self.speed = speed_multiplier or config.stream_speed_multiplier
        self.start_day = start_day
        self.producer: Optional[KafkaProducer] = None
        self.chaos = ChaosMonkey(config.chaos) if config.chaos.enabled else None

    def connect(self) -> None:
        """Connect to Kafka."""
        self.producer = KafkaProducer(
            bootstrap_servers=[self.config.kafka_bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        print(f"Connected to Kafka at {self.config.kafka_bootstrap_servers}")

    def close(self) -> None:
        """Close Kafka connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print("Kafka connection closed")

    def stream_from_parquet(self, parquet_path: str) -> dict:
        """Stream events from a parquet file to Kafka."""
        if not self.producer:
            self.connect()

        print(f"Loading events from {parquet_path}...")
        table = pq.read_table(parquet_path)
        df = table.to_pandas()

        # Filter to start_day if specified
        if self.start_day > 0:
            start_ts = df["ts_seconds"].min() + (self.start_day * 86400)
            df = df[df["ts_seconds"] >= start_ts]
            print(f"Starting from day {self.start_day} ({len(df):,} events remaining)")

        # Sort by timestamp
        df = df.sort_values(["ts_seconds", "sequence"])

        total_events = len(df)
        print(f"Streaming {total_events:,} events at {self.speed}x speed...")
        print(f"Topic: {self.config.kafka_topic}")
        print()

        sent_count = 0
        start_real_time = time.time()
        first_event_ts = df["ts_seconds"].iloc[0]

        for idx, row in df.iterrows():
            event = {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "ts": row["ts"],
                "ts_seconds": int(row["ts_seconds"]),
                "location_id": (
                    int(row["location_id"]) if pd.notna(row["location_id"]) else None
                ),
                "order_id": row["order_id"],
                "sequence": int(row["sequence"]),
                "body": row["body"],
            }

            # Apply chaos if enabled
            events_to_send = [event]
            if self.chaos:
                events_to_send = self.chaos.process(event)

            for e in events_to_send:
                # Calculate when this event should be sent based on speed multiplier
                event_offset = (e["ts_seconds"] - first_event_ts) / self.speed
                target_real_time = start_real_time + event_offset
                current_real_time = time.time()

                # Wait if we're ahead of schedule
                if current_real_time < target_real_time:
                    sleep_time = target_real_time - current_real_time
                    if sleep_time > 0.001:  # Only sleep if > 1ms
                        time.sleep(sleep_time)

                # Send to Kafka
                self.producer.send(
                    self.config.kafka_topic,
                    key=e.get("order_id"),
                    value=e,
                )
                sent_count += 1

                # Progress logging every 1000 events
                if sent_count % 1000 == 0:
                    elapsed = time.time() - start_real_time
                    rate = sent_count / elapsed if elapsed > 0 else 0
                    pct = (sent_count / total_events) * 100
                    print(f"  Sent {sent_count:,}/{total_events:,} ({pct:.1f}%) - {rate:.0f} events/sec")

        # Flush remaining chaos events
        if self.chaos:
            remaining = self.chaos.flush()
            for e in remaining:
                self.producer.send(
                    self.config.kafka_topic,
                    key=e.get("order_id"),
                    value=e,
                )
                sent_count += 1

        self.producer.flush()
        elapsed = time.time() - start_real_time

        print()
        print(f"Done! Sent {sent_count:,} events in {elapsed:.1f} seconds")
        print(f"Average rate: {sent_count / elapsed:.0f} events/sec")

        return {
            "events_sent": sent_count,
            "elapsed_seconds": round(elapsed, 1),
            "events_per_second": round(sent_count / elapsed, 0),
        }


def stream_events(
    config: GeneratorConfig,
    speed_multiplier: Optional[int] = None,
    start_day: int = 0,
) -> dict:
    """Stream events from generated parquet to Kafka."""
    # Find the parquet file
    events_path = Path(config.output_dir) / "events" / f"orders_{config.days}d.parquet"
    if not events_path.exists():
        raise FileNotFoundError(
            f"Events file not found: {events_path}\n"
            f"Run 'lakehouse testdata generate' first."
        )

    producer = StreamingProducer(config, speed_multiplier, start_day)
    try:
        producer.connect()
        return producer.stream_from_parquet(str(events_path))
    finally:
        producer.close()


def stream_realtime(config: GeneratorConfig) -> None:
    """Generate and stream events in real-time (not from parquet)."""
    from .events import generate_all_events, event_to_dict

    producer = KafkaProducer(
        bootstrap_servers=[config.kafka_bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    chaos = ChaosMonkey(config.chaos) if config.chaos.enabled else None

    print(f"Streaming real-time events to {config.kafka_topic}...")
    print("Press Ctrl+C to stop")
    print()

    sent_count = 0
    try:
        for event in generate_all_events(config):
            event_dict = event_to_dict(event)

            events_to_send = [event_dict]
            if chaos:
                events_to_send = chaos.process(event_dict)

            for e in events_to_send:
                producer.send(
                    config.kafka_topic,
                    key=e.get("order_id"),
                    value=e,
                )
                sent_count += 1

                if sent_count % 100 == 0:
                    print(f"Sent {sent_count:,} events...")

    except KeyboardInterrupt:
        print(f"\nStopped. Sent {sent_count:,} events.")
    finally:
        producer.flush()
        producer.close()
