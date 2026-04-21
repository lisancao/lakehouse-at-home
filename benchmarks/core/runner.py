"""Main benchmark execution engine."""

import os
import uuid
import tempfile
import time
from typing import Dict, Type, Any

from pyspark.sql import SparkSession, DataFrame

from ..config import BenchmarkConfig, BenchmarkResult, OperationType, ProcessingMode
from .metrics import MetricsCollector
from .reporter import BenchmarkReporter
from .data_generator import generate_benchmark_data


class BenchmarkRunner:
    """Execute benchmarks across multiple sources and scales."""

    def __init__(
        self,
        spark: SparkSession,
        config: BenchmarkConfig,
    ):
        self.spark = spark
        self.config = config
        self.collector = MetricsCollector()
        self.reporter = BenchmarkReporter(config.output_dir)
        self.run_id = str(uuid.uuid4())[:8]

        # Create temp directory for benchmark artifacts
        self.temp_dir = tempfile.mkdtemp(prefix="lakehouse_benchmark_")

        # Import source benchmarks lazily to avoid circular imports
        self._source_registry: Dict[str, Type] = {}

    def _get_source_registry(self) -> Dict[str, Type]:
        """Get or initialize the source registry."""
        if not self._source_registry:
            from ..sources.parquet import ParquetBenchmark
            from ..sources.csv import CSVBenchmark
            from ..sources.json import JSONBenchmark
            from ..sources.orc import ORCBenchmark
            from ..sources.iceberg import IcebergBenchmark
            from ..sources.delta import DeltaBenchmark

            self._source_registry = {
                "parquet": ParquetBenchmark,
                "csv": CSVBenchmark,
                "json": JSONBenchmark,
                "orc": ORCBenchmark,
                "iceberg": IcebergBenchmark,
                "delta": DeltaBenchmark,
            }
        return self._source_registry

    def run_all(self) -> Dict[str, Any]:
        """Run all configured benchmarks."""
        print("=" * 60)
        print("LAKEHOUSE BENCHMARK SUITE")
        print(f"Run ID: {self.run_id}")
        print("=" * 60)
        print(f"Sources: {', '.join(self.config.sources)}")
        print(f"Scales: {', '.join(str(s) for s in self.config.row_counts)}")
        print(f"Runs per benchmark: {self.config.benchmark_runs}")
        print()

        source_registry = self._get_source_registry()

        for source_name in self.config.sources:
            if source_name not in source_registry:
                print(f"Warning: Unknown source '{source_name}', skipping")
                continue

            self._run_source_benchmarks(source_name, source_registry)

        # Generate reports
        summary = self._generate_summary()
        self.reporter.write_results(
            self.collector.results,
            self.run_id,
            self.config.output_format,
        )
        self.reporter.print_summary(self.collector.results)

        return summary

    def _run_source_benchmarks(
        self, source_name: str, source_registry: Dict[str, Type]
    ) -> None:
        """Run all benchmarks for a single source."""
        print(f"\n--- Benchmarking: {source_name.upper()} ---")

        benchmark_class = source_registry[source_name]
        benchmark = benchmark_class(
            spark=self.spark,
            config=self.config,
            temp_dir=self.temp_dir,
        )

        # Setup for Iceberg
        if source_name == "iceberg":
            try:
                benchmark.setup_catalog()
            except Exception as e:
                print(f"Warning: Could not setup Iceberg catalog: {e}")
                print("Skipping Iceberg benchmarks")
                return

        for row_count in self.config.row_counts:
            print(f"\n  Scale: {row_count:,} rows")

            # Generate test data
            df = generate_benchmark_data(self.spark, row_count, self.config.seed)
            df.cache()
            df.count()  # Materialize cache

            path = f"{source_name}_{row_count}"

            # Run write benchmark
            if OperationType.WRITE in self.config.operations:
                if OperationType.WRITE in benchmark.supported_operations:
                    self._run_with_warmup(
                        benchmark,
                        "write",
                        df,
                        path,
                        row_count,
                    )

            # Run read benchmark
            if OperationType.READ in self.config.operations:
                if OperationType.READ in benchmark.supported_operations:
                    self._run_with_warmup(
                        benchmark,
                        "read",
                        df,
                        path,
                        row_count,
                    )

            df.unpersist()

    def _run_with_warmup(
        self,
        benchmark,
        operation: str,
        df: DataFrame,
        path: str,
        row_count: int,
    ) -> None:
        """Run benchmark with warmup and multiple iterations."""
        op_name = operation.upper()

        # Warmup runs
        print(f"    {op_name}: warmup...", end="", flush=True)
        for _ in range(self.config.warmup_runs):
            if operation == "write":
                benchmark.benchmark_write(df, path + "_warmup", row_count)
            else:
                # Ensure data exists for read
                if not benchmark.data_exists(path):
                    benchmark.write_data(df, path)
                benchmark.benchmark_read(path, row_count)

        # Benchmark runs
        print(f" running {self.config.benchmark_runs}x...", end="", flush=True)
        for i in range(self.config.benchmark_runs):
            if operation == "write":
                result = benchmark.benchmark_write(df, f"{path}_run{i}", row_count)
            else:
                if not benchmark.data_exists(path):
                    benchmark.write_data(df, path)
                result = benchmark.benchmark_read(path, row_count)

            result.run_id = self.run_id
            result.spark_version = self.spark.version
            self.collector.add_result(result)

        # Print quick summary
        recent_results = self.collector.results[-self.config.benchmark_runs :]
        avg_duration = sum(r.duration_seconds for r in recent_results) / len(
            recent_results
        )
        avg_throughput = sum(r.rows_per_second for r in recent_results) / len(
            recent_results
        )
        print(f" {avg_duration:.2f}s ({avg_throughput:,.0f} rows/s)")

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate benchmark summary."""
        summary = {
            "run_id": self.run_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "config": {
                "sources": self.config.sources,
                "row_counts": self.config.row_counts,
                "benchmark_runs": self.config.benchmark_runs,
            },
            "by_source": self.collector.aggregate_by_source(),
            "by_scale": self.collector.aggregate_by_scale(),
            "total_results": len(self.collector.results),
        }

        return summary

    def cleanup(self) -> None:
        """Clean up temporary benchmark files."""
        import shutil

        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
