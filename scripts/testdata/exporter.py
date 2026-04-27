"""Parquet batch exporter for Iceberg loading."""

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from .config import GeneratorConfig
from .events import generate_all_events, event_to_dict
from .chaos import apply_chaos


def export_events_to_parquet(config: GeneratorConfig) -> dict:
    """Generate all events and export to a parquet file using streaming writes."""
    output_dir = Path(config.output_dir) / "events"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"orders_{config.days}d.parquet"

    print(f"Generating {config.days} days of order events...")
    print(f"  Start date: {config.start_date}")
    print(f"  Locations: {len(config.locations)}")
    print(f"  Base orders/day: {config.base_orders_per_day}")
    print(f"  Chaos enabled: {config.chaos.enabled}")
    print()

    # Define schema
    schema = pa.schema([
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("ts", pa.string()),
        ("ts_seconds", pa.int64()),
        ("location_id", pa.int32()),
        ("order_id", pa.string()),
        ("sequence", pa.int32()),
        ("body", pa.string()),
    ])

    # Write in batches using ParquetWriter
    batch_size = 50000
    batch: List[dict] = []
    total_events = 0
    unique_orders = set()

    writer = pq.ParquetWriter(output_path, schema, compression="snappy")

    try:
        for event in generate_all_events(config):
            event_dict = event_to_dict(event)
            batch.append(event_dict)
            unique_orders.add(event_dict.get("order_id"))

            if len(batch) >= batch_size:
                # Apply chaos to batch
                if config.chaos.enabled:
                    batch = apply_chaos(batch, config.chaos)

                # Sort batch by timestamp
                batch.sort(key=lambda e: (e.get("ts_seconds", 0), e.get("sequence", 0)))

                # Write batch
                _write_batch(writer, batch, schema)
                total_events += len(batch)
                print(f"  Written {total_events:,} events...")
                batch = []

        # Write remaining events
        if batch:
            if config.chaos.enabled:
                batch = apply_chaos(batch, config.chaos)
            batch.sort(key=lambda e: (e.get("ts_seconds", 0), e.get("sequence", 0)))
            _write_batch(writer, batch, schema)
            total_events += len(batch)

    finally:
        writer.close()

    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    print()
    print(f"Done! Total events: {total_events:,}")
    print(f"File size: {file_size_mb:.1f} MB")

    return {
        "path": str(output_path),
        "events": total_events,
        "orders": len(unique_orders),
        "file_size_mb": round(file_size_mb, 1),
    }


def _write_batch(writer: pq.ParquetWriter, batch: List[dict], schema: pa.Schema) -> None:
    """Write a batch of events to the parquet writer."""
    table = pa.table({
        "event_id": pa.array([e["event_id"] for e in batch], type=pa.string()),
        "event_type": pa.array([e["event_type"] for e in batch], type=pa.string()),
        "ts": pa.array([e["ts"] for e in batch], type=pa.string()),
        "ts_seconds": pa.array([e["ts_seconds"] for e in batch], type=pa.int64()),
        "location_id": pa.array([e.get("location_id") for e in batch], type=pa.int32()),
        "order_id": pa.array([e.get("order_id") for e in batch], type=pa.string()),
        "sequence": pa.array([e["sequence"] for e in batch], type=pa.int32()),
        "body": pa.array([e["body"] for e in batch], type=pa.string()),
    }, schema=schema)
    writer.write_table(table)


def load_events_from_parquet(path: str) -> pa.Table:
    """Load events from a parquet file."""
    return pq.read_table(path)


def get_event_stats(path: str) -> dict:
    """Get statistics about a generated events file."""
    import pyarrow.compute as pc

    table = pq.read_table(path)

    # Get unique counts using pyarrow compute
    order_ids = table.column("order_id")
    unique_orders = len(pc.unique(order_ids))

    # Event type counts
    event_types = table.column("event_type")
    event_type_counts = {}
    for et in pc.unique(event_types).to_pylist():
        mask = pc.equal(event_types, et)
        event_type_counts[et] = pc.sum(pc.cast(mask, pa.int64())).as_py()

    # Location counts
    locations = table.column("location_id")
    location_counts = {}
    for loc in pc.unique(locations).to_pylist():
        if loc is not None:
            mask = pc.equal(locations, loc)
            location_counts[int(loc)] = pc.sum(pc.cast(mask, pa.int64())).as_py()

    # Date range
    ts_col = table.column("ts")
    ts_min = pc.min(ts_col).as_py()
    ts_max = pc.max(ts_col).as_py()

    stats = {
        "total_events": table.num_rows,
        "unique_orders": unique_orders,
        "event_types": event_type_counts,
        "locations": location_counts,
        "date_range": {
            "min": ts_min,
            "max": ts_max,
        },
    }

    return stats


def generate_iceberg_load_script(config: GeneratorConfig) -> str:
    """Generate a PySpark script to load data into Iceberg tables."""
    # Use absolute container paths (./data is mounted as /data in container)
    events_path = f"/data/events/orders_{config.days}d.parquet"
    dims_path = "/data/dimensions"

    script = f'''"""Load generated test data into Iceberg bronze tables."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName("LoadTestData").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Loading test data into Iceberg bronze tables...")

# Load dimension tables
print("\\n1. Loading dimension tables...")

dims_path = "{dims_path}"

# Categories
spark.read.parquet(f"{{dims_path}}/categories.parquet").write.mode("overwrite").saveAsTable("iceberg.bronze.dim_categories")
print("   - iceberg.bronze.dim_categories")

# Brands
spark.read.parquet(f"{{dims_path}}/brands.parquet").write.mode("overwrite").saveAsTable("iceberg.bronze.dim_brands")
print("   - iceberg.bronze.dim_brands")

# Items
spark.read.parquet(f"{{dims_path}}/items.parquet").write.mode("overwrite").saveAsTable("iceberg.bronze.dim_items")
print("   - iceberg.bronze.dim_items")

# Locations
spark.read.parquet(f"{{dims_path}}/locations.parquet").write.mode("overwrite").saveAsTable("iceberg.bronze.dim_locations")
print("   - iceberg.bronze.dim_locations")

# Load events
print("\\n2. Loading events table...")
events_path = "{events_path}"

events_df = spark.read.parquet(events_path)

# Parse timestamp string to timestamp type (handle both with and without microseconds)
events_df = events_df.withColumn(
    "event_timestamp",
    f.coalesce(
        f.try_to_timestamp(events_df.ts, f.lit("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")),
        f.try_to_timestamp(events_df.ts, f.lit("yyyy-MM-dd'T'HH:mm:ss"))
    )
)

events_df.write.mode("overwrite").saveAsTable("iceberg.bronze.orders")
print(f"   - iceberg.bronze.orders ({{events_df.count():,}} events)")

print("\\nDone! Tables created:")
print("  - iceberg.bronze.dim_categories")
print("  - iceberg.bronze.dim_brands")
print("  - iceberg.bronze.dim_items")
print("  - iceberg.bronze.dim_locations")
print("  - iceberg.bronze.orders")

spark.stop()
'''
    return script
