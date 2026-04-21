"""CLI entry point for benchmark suite.

Usage:
    python -m benchmarks run [options]
    python -m benchmarks list
"""

import argparse
import sys
from typing import List, Optional

from pyspark.sql import SparkSession

from .config import BenchmarkConfig, OperationType, DataScale


def create_spark_session(config: BenchmarkConfig) -> SparkSession:
    """Create Spark session for benchmarks."""
    builder = SparkSession.builder.appName("LakehouseBenchmark")

    # Apply config settings
    for key, value in config.spark_config.items():
        builder = builder.config(key, value)

    # Enable Iceberg support
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    return builder.getOrCreate()


def parse_sources(sources_str: Optional[str]) -> List[str]:
    """Parse comma-separated sources."""
    if not sources_str:
        return ["parquet", "csv", "json", "orc", "iceberg"]

    valid_sources = {"parquet", "csv", "json", "orc", "iceberg", "delta"}
    sources = [s.strip().lower() for s in sources_str.split(",")]

    for s in sources:
        if s not in valid_sources:
            print(f"Warning: Unknown source '{s}', skipping")

    return [s for s in sources if s in valid_sources]


def parse_scales(scales_str: Optional[str]) -> List[int]:
    """Parse comma-separated scales."""
    if not scales_str:
        return [DataScale.SMALL.value, DataScale.MEDIUM.value]

    scales = []
    for s in scales_str.split(","):
        s = s.strip().upper()
        # Check if it's a named scale
        if hasattr(DataScale, s):
            scales.append(getattr(DataScale, s).value)
        else:
            try:
                scales.append(int(s))
            except ValueError:
                print(f"Warning: Invalid scale '{s}', skipping")

    return scales if scales else [DataScale.SMALL.value, DataScale.MEDIUM.value]


def parse_operations(ops_str: Optional[str]) -> List[OperationType]:
    """Parse comma-separated operations."""
    if not ops_str:
        return [OperationType.READ, OperationType.WRITE]

    valid_ops = {"read": OperationType.READ, "write": OperationType.WRITE}
    operations = []

    for op in ops_str.split(","):
        op = op.strip().lower()
        if op in valid_ops:
            operations.append(valid_ops[op])
        else:
            print(f"Warning: Unknown operation '{op}', skipping")

    return operations if operations else [OperationType.READ, OperationType.WRITE]


def cmd_run(args: argparse.Namespace) -> int:
    """Run benchmarks."""
    from .core.runner import BenchmarkRunner

    sources = parse_sources(args.sources)
    scales = parse_scales(args.scales)
    operations = parse_operations(args.operations)

    config = BenchmarkConfig(
        sources=sources,
        row_counts=scales,
        operations=operations,
        warmup_runs=args.warmup,
        benchmark_runs=args.runs,
        output_dir=args.output_dir,
        output_format=args.format,
    )

    spark = create_spark_session(config)
    runner = None

    try:
        runner = BenchmarkRunner(spark, config)
        summary = runner.run_all()

        print(f"\nBenchmark complete. Total results: {summary['total_results']}")
        return 0

    except KeyboardInterrupt:
        print("\nBenchmark interrupted.")
        return 1

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    finally:
        if runner:
            runner.cleanup()
        spark.stop()


def cmd_list(args: argparse.Namespace) -> int:
    """List available sources and scales."""
    print("Available Sources:")
    print("  - parquet  : Apache Parquet columnar format")
    print("  - csv      : Comma-separated values")
    print("  - json     : JSON lines format")
    print("  - orc      : Optimized Row Columnar format")
    print("  - iceberg  : Apache Iceberg table format")
    print("  - delta    : Delta Lake table format")

    print("\nAvailable Scales:")
    for scale in DataScale:
        print(f"  - {scale.name:<8}: {scale.value:>10,} rows")

    print("\nAvailable Operations:")
    print("  - read     : Read benchmark")
    print("  - write    : Write benchmark")

    return 0


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="benchmarks",
        description="Lakehouse Benchmark Suite",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run benchmarks")
    run_parser.add_argument(
        "--sources",
        "-s",
        help="Comma-separated list of sources (default: parquet,csv,json,orc,iceberg)",
    )
    run_parser.add_argument(
        "--scales",
        help="Comma-separated list of row counts or scale names (default: SMALL,MEDIUM)",
    )
    run_parser.add_argument(
        "--operations",
        "-o",
        help="Comma-separated list of operations (default: read,write)",
    )
    run_parser.add_argument(
        "--warmup",
        "-w",
        type=int,
        default=1,
        help="Number of warmup runs (default: 1)",
    )
    run_parser.add_argument(
        "--runs",
        "-r",
        type=int,
        default=3,
        help="Number of benchmark runs (default: 3)",
    )
    run_parser.add_argument(
        "--output-dir",
        "-d",
        default="benchmarks/results",
        help="Output directory for results (default: benchmarks/results)",
    )
    run_parser.add_argument(
        "--format",
        "-f",
        choices=["json", "csv", "both"],
        default="json",
        help="Output format (default: json)",
    )
    run_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    run_parser.set_defaults(func=cmd_run)

    # List command
    list_parser = subparsers.add_parser("list", help="List available sources and scales")
    list_parser.set_defaults(func=cmd_list)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
