"""CLI entry point for test data generation."""

import argparse
import sys
from datetime import date
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.testdata import (
    GeneratorConfig,
    ChaosConfig,
    generate_dataset,
    stream_events,
    save_dimensions,
    export_events_to_parquet,
)


def cmd_generate(args):
    """Generate test data."""
    chaos_config = ChaosConfig(
        enabled=not args.no_chaos,
        null_rate=args.chaos_rate,
        late_event_rate=args.chaos_rate * 0.6,
        duplicate_rate=args.chaos_rate * 0.4,
        malformed_json_rate=args.chaos_rate * 0.2,
    )

    config = GeneratorConfig(
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        seed=args.seed,
        base_orders_per_day=args.orders_per_day,
        cancel_rate=args.cancel_rate,
        chaos=chaos_config,
        output_dir=args.output,
        output_name=args.name,
    )

    generate_dataset(config)


def cmd_stream(args):
    """Stream events to Kafka."""
    config = GeneratorConfig(
        days=args.days,
        kafka_bootstrap_servers=args.kafka,
        kafka_topic=args.topic,
        output_dir=args.output,
        chaos=ChaosConfig(enabled=not args.no_chaos),
    )

    stream_events(config, speed_multiplier=args.speed, start_day=args.start_day)


def cmd_clean(args):
    """Clean generated data."""
    output_dir = Path(args.output)

    dirs_to_clean = [
        output_dir / "dimensions",
        output_dir / "events",
    ]

    files_to_clean = [
        output_dir / "load_to_iceberg.py",
    ]

    for d in dirs_to_clean:
        if d.exists():
            import shutil
            shutil.rmtree(d)
            print(f"Removed: {d}")

    for f in files_to_clean:
        if f.exists():
            f.unlink()
            print(f"Removed: {f}")

    print("Clean complete.")


def cmd_stats(args):
    """Show statistics about generated data."""
    from scripts.testdata.exporter import get_event_stats

    events_path = Path(args.output) / "events"
    parquet_files = list(events_path.glob("*.parquet"))

    if not parquet_files:
        print("No generated data found. Run 'generate' first.")
        return

    for path in parquet_files:
        print(f"\n{path.name}:")
        print("-" * 40)

        stats = get_event_stats(str(path))

        print(f"Total events:  {stats['total_events']:,}")
        print(f"Unique orders: {stats['unique_orders']:,}")
        print(f"Date range:    {stats['date_range']['min'][:10]} to {stats['date_range']['max'][:10]}")

        print("\nEvents by type:")
        for event_type, count in sorted(stats["event_types"].items()):
            print(f"  {event_type}: {count:,}")

        print("\nEvents by location:")
        for loc_id, count in sorted(stats["locations"].items()):
            print(f"  Location {loc_id}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Lakehouse test data generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 90 days of data
  python -m scripts.testdata generate

  # Generate 30 days with custom settings
  python -m scripts.testdata generate --days 30 --orders-per-day 500

  # Stream to Kafka at 100x speed
  python -m scripts.testdata stream --speed 100

  # Clean generated data
  python -m scripts.testdata clean
""",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Generate command
    gen_parser = subparsers.add_parser("generate", help="Generate test data")
    gen_parser.add_argument("--days", type=int, default=90, help="Number of days to generate")
    gen_parser.add_argument("--seed", type=int, default=42, help="Random seed")
    gen_parser.add_argument("--orders-per-day", type=int, default=835, help="Base orders per day")
    gen_parser.add_argument("--cancel-rate", type=float, default=0.0,
                            help="Fraction of orders cancelled mid-lifecycle (emits order_cancelled)")
    gen_parser.add_argument("--start-date", default="2024-01-01", help="Start date (YYYY-MM-DD)")
    gen_parser.add_argument("--output", default="data", help="Output directory")
    gen_parser.add_argument("--name", default=None,
                            help="Override events filename (default orders_{days}d.parquet)")
    gen_parser.add_argument("--no-chaos", action="store_true", help="Disable chaos injection")
    gen_parser.add_argument("--chaos-rate", type=float, default=0.05, help="Chaos injection rate")
    gen_parser.set_defaults(func=cmd_generate)

    # Stream command
    stream_parser = subparsers.add_parser("stream", help="Stream events to Kafka")
    stream_parser.add_argument("--speed", type=int, default=60, help="Speed multiplier (1=realtime)")
    stream_parser.add_argument("--start-day", type=int, default=0, help="Day to start from")
    stream_parser.add_argument("--kafka", default="localhost:9092", help="Kafka bootstrap servers")
    stream_parser.add_argument("--topic", default="orders", help="Kafka topic")
    stream_parser.add_argument("--days", type=int, default=90, help="Days in generated file")
    stream_parser.add_argument("--output", default="data", help="Data directory")
    stream_parser.add_argument("--no-chaos", action="store_true", help="Disable chaos during stream")
    stream_parser.set_defaults(func=cmd_stream)

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean generated data")
    clean_parser.add_argument("--output", default="data", help="Data directory")
    clean_parser.set_defaults(func=cmd_clean)

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Show data statistics")
    stats_parser.add_argument("--output", default="data", help="Data directory")
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
