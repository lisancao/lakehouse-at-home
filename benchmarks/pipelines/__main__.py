"""CLI for pipeline benchmarks.

Usage:
    python -m benchmarks.pipelines file [options]   # Local test (no Kafka)
    python -m benchmarks.pipelines kafka [options]  # Requires Kafka
    python -m benchmarks.pipelines --help
"""

import argparse
import sys

from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    """Create Spark session for benchmarks."""
    return (
        SparkSession.builder
        .appName("PipelineBenchmark")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )


def cmd_iceberg(args: argparse.Namespace) -> int:
    """Run Iceberg pipeline benchmark."""
    from .iceberg_benchmark import IcebergPipelineBenchmark

    spark = create_spark_session()

    try:
        benchmark = IcebergPipelineBenchmark(spark=spark)
        results = benchmark.run_comparison(row_count=args.rows)

        # Save results
        import json
        import os
        from datetime import datetime, timezone

        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(
            args.output_dir,
            f"pipeline_iceberg_{benchmark.run_id}_{timestamp}.json"
        )

        data = {
            "metadata": {
                "run_id": benchmark.run_id,
                "benchmark_type": "iceberg_pipeline",
                "row_count": args.rows,
            },
            "results": {k: v.to_dict() for k, v in results.items()},
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\nResults saved to: {filepath}")
        return 0

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    finally:
        benchmark.cleanup()
        spark.stop()


def cmd_file(args: argparse.Namespace) -> int:
    """Run file-based pipeline benchmark (no external dependencies)."""
    from .file_benchmark import FilePipelineBenchmark

    spark = create_spark_session()

    try:
        benchmark = FilePipelineBenchmark(spark=spark)
        results = benchmark.run_comparison(
            row_count=args.rows,
            batch_count=args.batches,
        )

        # Save results
        import json
        import os
        from datetime import datetime, timezone

        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(
            args.output_dir,
            f"pipeline_file_{benchmark.run_id}_{timestamp}.json"
        )

        data = {
            "metadata": {
                "run_id": benchmark.run_id,
                "benchmark_type": "file_pipeline",
                "row_count": args.rows,
                "batch_count": args.batches,
            },
            "results": {k: v.to_dict() for k, v in results.items()},
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\nResults saved to: {filepath}")
        print(f"Benchmark complete. {len(results)} pipelines tested.")
        return 0

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    finally:
        benchmark.cleanup()
        spark.stop()


def cmd_udf(args: argparse.Namespace) -> int:
    """Run UDF performance benchmark."""
    from .udf_benchmark import UdfPipelineBenchmark

    spark = create_spark_session()

    try:
        benchmark = UdfPipelineBenchmark(
            spark=spark,
            warmup_runs=args.warmup,
            benchmark_runs=args.runs,
        )
        results = benchmark.run_comparison(row_count=args.rows)

        # Save results
        import json
        import os
        from datetime import datetime, timezone

        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(
            args.output_dir,
            f"pipeline_udf_{benchmark.run_id}_{timestamp}.json",
        )

        data = {
            "metadata": {
                "run_id": benchmark.run_id,
                "benchmark_type": "udf_pipeline",
                "row_count": args.rows,
                "warmup_runs": args.warmup,
                "benchmark_runs": args.runs,
            },
            "results": {k: v.to_dict() for k, v in results.items()},
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\nResults saved to: {filepath}")
        return 0

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    finally:
        benchmark.cleanup()
        spark.stop()


def cmd_kafka(args: argparse.Namespace) -> int:
    """Run Kafka pipeline benchmark."""
    from .runner import PipelineBenchmarkRunner

    spark = create_spark_session()

    try:
        runner = PipelineBenchmarkRunner(
            spark=spark,
            output_dir=args.output_dir,
        )

        results = runner.run_kafka_benchmark(
            kafka_bootstrap_servers=args.kafka_servers,
            topic=args.topic,
            message_count=args.messages,
            duration_seconds=args.duration,
            trigger_interval=args.trigger_interval,
        )

        print(f"\nBenchmark complete. {len(results)} pipelines tested.")
        return 0

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    finally:
        spark.stop()


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="benchmarks.pipelines",
        description="Pipeline Benchmark: Imperative vs Declarative",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Iceberg benchmark
    iceberg_parser = subparsers.add_parser(
        "iceberg",
        help="Run Iceberg pipeline benchmark (read/write)",
    )
    iceberg_parser.add_argument(
        "--rows",
        "-r",
        type=int,
        default=10000,
        help="Number of rows to process (default: 10000)",
    )
    iceberg_parser.add_argument(
        "--output-dir",
        default="benchmarks/results/pipelines",
        help="Output directory for results",
    )
    iceberg_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    iceberg_parser.set_defaults(func=cmd_iceberg)

    # File benchmark (no external dependencies)
    file_parser = subparsers.add_parser(
        "file",
        help="Run file-based pipeline benchmark (no Kafka required)",
    )
    file_parser.add_argument(
        "--rows",
        "-r",
        type=int,
        default=10000,
        help="Number of rows to process (default: 10000)",
    )
    file_parser.add_argument(
        "--batches",
        "-b",
        type=int,
        default=5,
        help="Number of batches (default: 5)",
    )
    file_parser.add_argument(
        "--output-dir",
        default="benchmarks/results/pipelines",
        help="Output directory for results",
    )
    file_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    file_parser.set_defaults(func=cmd_file)

    # UDF benchmark
    udf_parser = subparsers.add_parser(
        "udf",
        help="Run UDF performance benchmark (6 UDF types x 3 workloads)",
    )
    udf_parser.add_argument(
        "--rows",
        "-r",
        type=int,
        default=100000,
        help="Number of rows to process (default: 100000)",
    )
    udf_parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Warmup runs (default: 1)",
    )
    udf_parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Benchmark runs (default: 3)",
    )
    udf_parser.add_argument(
        "--output-dir",
        default="benchmarks/results/pipelines",
        help="Output directory for results",
    )
    udf_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    udf_parser.set_defaults(func=cmd_udf)

    # Kafka benchmark
    kafka_parser = subparsers.add_parser(
        "kafka",
        help="Run Kafka pipeline benchmark (requires Kafka)",
    )
    kafka_parser.add_argument(
        "--kafka-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    kafka_parser.add_argument(
        "--topic",
        default="benchmark_orders",
        help="Kafka topic (default: benchmark_orders)",
    )
    kafka_parser.add_argument(
        "--messages",
        "-m",
        type=int,
        default=1000,
        help="Number of messages to publish (default: 1000)",
    )
    kafka_parser.add_argument(
        "--duration",
        "-d",
        type=int,
        default=30,
        help="Duration per pipeline in seconds (default: 30)",
    )
    kafka_parser.add_argument(
        "--trigger-interval",
        default="5 seconds",
        help="Streaming trigger interval (default: '5 seconds')",
    )
    kafka_parser.add_argument(
        "--output-dir",
        default="benchmarks/results/pipelines",
        help="Output directory for results",
    )
    kafka_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    kafka_parser.set_defaults(func=cmd_kafka)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print("\nAvailable benchmarks:")
        print("  iceberg - Iceberg read/write benchmark (uses local Hadoop catalog)")
        print("  file    - File-based streaming benchmark (no external dependencies)")
        print("  udf     - UDF performance benchmark (6 UDF types x 3 workloads)")
        print("  kafka   - Kafka streaming benchmark (requires Kafka)")
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
