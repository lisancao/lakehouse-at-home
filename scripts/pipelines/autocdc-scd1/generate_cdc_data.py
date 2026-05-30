"""Generate a small CDC change feed for the AUTO CDC -> SCD1 demo.

Writes newline-delimited JSON files into /tmp/autocdc-scd1/cdc-events so the
pipeline's streaming file source can pick them up.

The feed is crafted to exercise every SCD Type 1 behavior:
    - id=1: inserted, then updated to a newer value (Boston, seq=3) AND later
            receives a STALE out-of-order event (Chicago, seq=2). SCD1 +
            sequence_by must keep Boston (highest seq wins), not Chicago.
    - id=2: inserted, then deleted -> must be absent from the target.
    - id=3: inserted only -> stays as-is.

Expected final SCD1 state of spark_catalog.cdc_demo.scd1_customers:
    id=1  Alice  Boston
    id=3  Carol  San Francisco
    (id=2 removed)
"""

import json
import os
import shutil

CDC_DIR = "/tmp/autocdc-scd1/cdc-events"

# Each inner list becomes one JSON file (one "micro-batch" worth of events).
BATCHES = [
    # batch 01: initial inserts
    [
        {"id": 1, "name": "Alice", "city": "New York", "op": "UPSERT", "seq": 1},
        {"id": 2, "name": "Bob", "city": "Los Angeles", "op": "UPSERT", "seq": 1},
        {"id": 3, "name": "Carol", "city": "San Francisco", "op": "UPSERT", "seq": 1},
    ],
    # batch 02: update id=1 (newest, seq=3) and delete id=2
    [
        {"id": 1, "name": "Alice", "city": "Boston", "op": "UPSERT", "seq": 3},
        {"id": 2, "name": "Bob", "city": "Los Angeles", "op": "DELETE", "seq": 2},
    ],
    # batch 03: STALE out-of-order update for id=1 (seq=2 < 3) -> must be ignored
    [
        {"id": 1, "name": "Alice", "city": "Chicago", "op": "UPSERT", "seq": 2},
    ],
]


def main() -> None:
    if os.path.exists(CDC_DIR):
        shutil.rmtree(CDC_DIR)
    os.makedirs(CDC_DIR, exist_ok=True)

    for i, batch in enumerate(BATCHES, start=1):
        path = os.path.join(CDC_DIR, f"events_{i:02d}.json")
        with open(path, "w") as fh:
            for event in batch:
                fh.write(json.dumps(event) + "\n")
        print(f"wrote {len(batch):>2} events -> {path}")

    print(f"\nCDC feed ready in {CDC_DIR}")
    print("Expected SCD1 result: id=1 Boston, id=3 San Francisco, id=2 removed")


if __name__ == "__main__":
    main()
