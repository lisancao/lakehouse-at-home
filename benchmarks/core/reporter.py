"""Benchmark result reporting and output generation."""

import json
import csv
import os
from datetime import datetime, timezone
from typing import List, Dict, Any

from ..config import BenchmarkResult


class BenchmarkReporter:
    """Generate benchmark reports in various formats."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def write_results(
        self,
        results: List[BenchmarkResult],
        run_id: str,
        output_format: str = "json",
    ) -> Dict[str, str]:
        """Write benchmark results to files."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_name = f"benchmark_{run_id}_{timestamp}"

        output_files = {}

        if output_format in ("json", "both"):
            json_path = os.path.join(self.output_dir, f"{base_name}.json")
            self._write_json(results, json_path, run_id)
            output_files["json"] = json_path
            print(f"\nResults written to: {json_path}")

        if output_format in ("csv", "both"):
            csv_path = os.path.join(self.output_dir, f"{base_name}.csv")
            self._write_csv(results, csv_path)
            output_files["csv"] = csv_path
            print(f"Results written to: {csv_path}")

        return output_files

    def _write_json(
        self, results: List[BenchmarkResult], path: str, run_id: str
    ) -> None:
        """Write results as JSON."""
        from .metrics import MetricsCollector

        collector = MetricsCollector()
        for r in results:
            collector.add_result(r)

        data = {
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "total_results": len(results),
                "run_id": run_id,
            },
            "results": [r.to_dict() for r in results],
            "summary": {
                "by_source": collector.aggregate_by_source(),
                "by_scale": {
                    str(k): v for k, v in collector.aggregate_by_scale().items()
                },
                "by_operation": collector.aggregate_by_operation(),
                "comparison": collector.get_comparison_table(),
            },
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def _write_csv(self, results: List[BenchmarkResult], path: str) -> None:
        """Write results as CSV."""
        fieldnames = [
            "source",
            "operation",
            "mode",
            "row_count",
            "duration_seconds",
            "rows_per_second",
            "mb_per_second",
            "file_size_bytes",
            "timestamp",
            "spark_version",
            "run_id",
        ]

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for r in results:
                writer.writerow(r.to_dict())

    def print_summary(self, results: List[BenchmarkResult]) -> None:
        """Print a summary table to console."""
        from .metrics import MetricsCollector

        collector = MetricsCollector()
        for r in results:
            collector.add_result(r)

        comparison = collector.get_comparison_table()

        print("\n" + "=" * 70)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 70)
        print(f"\n{'Source':<15} {'Avg Duration (s)':<18} {'Throughput (rows/s)':<20} {'Runs'}")
        print("-" * 70)

        for row in comparison:
            print(
                f"{row['source']:<15} {row['avg_duration_sec']:<18.3f} "
                f"{row['avg_throughput_rows_sec']:<20,.0f} {row['runs']}"
            )

        # Performance ranking
        if len(comparison) > 1:
            fastest = comparison[0]
            slowest = comparison[-1]
            speedup = slowest["avg_duration_sec"] / fastest["avg_duration_sec"]
            print(f"\nFastest: {fastest['source']} ({speedup:.1f}x faster than {slowest['source']})")
