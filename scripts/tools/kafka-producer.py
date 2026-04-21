"""Simple Kafka producer for ghost kitchen order events.

Generates realistic order lifecycle events matching the parquet schema.
For full replay from generated data, use: ./lakehouse testdata stream
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

# Ghost kitchen domain data
brands = list(range(1, 21))  # 20 ghost kitchen brands
locations = list(range(1, 5))  # 4 delivery cities
event_lifecycle = [
    "order_created",
    "kitchen_started",
    "kitchen_finished",
    "order_ready",
    "driver_arrived",
    "driver_picked_up",
    "delivered",
]

print("Streaming order events to Kafka topic 'orders'...")
print("Press Ctrl+C to stop\n")

counter = 0

try:
    while True:
        order_id = str(uuid.uuid4())[:8]
        location_id = random.choice(locations)
        brand_id = random.choice(brands)
        total = round(random.uniform(8.0, 65.0), 2)

        # Send full lifecycle for each order
        for seq, event_type in enumerate(event_lifecycle):
            now = datetime.now()
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "ts": now.isoformat(),
                "ts_seconds": int(now.timestamp()),
                "location_id": location_id,
                "order_id": order_id,
                "sequence": seq,
                "body": json.dumps({
                    "brand_id": brand_id,
                    "total": total,
                    "lat": round(random.uniform(37.7, 37.8), 6),
                    "lng": round(random.uniform(-122.5, -122.4), 6),
                    "driver_id": f"driver_{random.randint(1, 50)}",
                }),
            }

            producer.send("orders", key=order_id, value=event)
            counter += 1

            # Simulate time between lifecycle events (2-30 seconds)
            time.sleep(random.uniform(0.1, 0.5))

        print(f"  Order {order_id} complete ({counter} events total)")

        # Pause between orders
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print(f"\nStopped. Sent {counter} events.")
    producer.close()
