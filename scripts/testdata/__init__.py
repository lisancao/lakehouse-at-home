"""
Test data generation module for lakehouse-stack.

Generates realistic food delivery order data inspired by Casper's Kitchens.
Supports both batch (Parquet) and streaming (Kafka) workflows.

Usage:
    from scripts.testdata import GeneratorConfig, generate_dataset

    config = GeneratorConfig(days=90)
    generate_dataset(config)
"""

from .config import GeneratorConfig, ChaosConfig, ServiceTimes, DemandPattern
from .dimensions import save_dimensions, get_brands, get_items, get_categories
from .events import generate_all_events, Event
from .exporter import export_events_to_parquet, generate_iceberg_load_script
from .producer import stream_events, StreamingProducer

__all__ = [
    # Config
    "GeneratorConfig",
    "ChaosConfig",
    "ServiceTimes",
    "DemandPattern",
    # Dimensions
    "save_dimensions",
    "get_brands",
    "get_items",
    "get_categories",
    # Events
    "generate_all_events",
    "Event",
    # Export
    "export_events_to_parquet",
    "generate_iceberg_load_script",
    # Streaming
    "stream_events",
    "StreamingProducer",
]


def generate_dataset(config: GeneratorConfig = None) -> dict:
    """Generate a complete test dataset (dimensions + events)."""
    if config is None:
        config = GeneratorConfig()

    print("=" * 60)
    print("LAKEHOUSE TEST DATA GENERATOR")
    print("=" * 60)
    print()

    # Generate dimensions
    print("1. Generating dimension tables...")
    dim_results = save_dimensions(config)
    for table, count in dim_results.items():
        print(f"   - {table}: {count} records")
    print()

    # Generate events
    print("2. Generating event data...")
    event_results = export_events_to_parquet(config)
    print()

    # Generate load script
    print("3. Generating Iceberg load script...")
    load_script = generate_iceberg_load_script(config)
    load_script_path = f"{config.output_dir}/load_to_iceberg.py"
    with open(load_script_path, "w") as f:
        f.write(load_script)
    print(f"   - {load_script_path}")
    print()

    print()
    print("=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print()
    print(f"Dimensions: {config.output_dir}/dimensions/")
    print(f"Events:     {event_results['path']}")
    print(f"Orders:     {event_results['orders']:,}")
    print(f"Events:     {event_results['events']:,}")
    print(f"File size:  {event_results['file_size_mb']} MB")
    print()
    print("Next steps:")
    print("  1. Load to Iceberg:  ./lakehouse testdata load")
    print("  2. Stream to Kafka:  ./lakehouse testdata stream --speed 60")
    print()

    return {
        "dimensions": dim_results,
        "events": event_results,
        "load_script": load_script_path,
    }
