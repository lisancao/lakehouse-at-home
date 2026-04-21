"""Metrics collection and aggregation for benchmarks."""

import time
import statistics
from typing import List, Dict, Any

from ..config import BenchmarkResult


class TimingContext:
    """Context manager for timing operations."""

    def __init__(self):
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.duration: float = 0.0

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time


class MetricsCollector:
    """Collect and aggregate benchmark metrics."""

    def __init__(self):
        self.results: List[BenchmarkResult] = []

    def add_result(self, result: BenchmarkResult) -> None:
        """Add a benchmark result."""
        self.results.append(result)

    def aggregate_by_source(self) -> Dict[str, Dict[str, Any]]:
        """Aggregate results by source."""
        aggregated = {}

        for source in set(r.source for r in self.results):
            source_results = [r for r in self.results if r.source == source]
            aggregated[source] = self._aggregate_results(source_results)

        return aggregated

    def aggregate_by_scale(self) -> Dict[int, Dict[str, Any]]:
        """Aggregate results by row count."""
        aggregated = {}

        for row_count in set(r.row_count for r in self.results):
            scale_results = [r for r in self.results if r.row_count == row_count]
            aggregated[row_count] = self._aggregate_results(scale_results)

        return aggregated

    def aggregate_by_operation(self) -> Dict[str, Dict[str, Any]]:
        """Aggregate results by operation type."""
        aggregated = {}

        for op in set(r.operation.value for r in self.results):
            op_results = [r for r in self.results if r.operation.value == op]
            aggregated[op] = self._aggregate_results(op_results)

        return aggregated

    def _aggregate_results(self, results: List[BenchmarkResult]) -> Dict[str, Any]:
        """Compute statistical aggregates for a set of results."""
        if not results:
            return {}

        durations = [r.duration_seconds for r in results]
        throughputs = [r.rows_per_second for r in results]
        file_sizes = [r.file_size_bytes for r in results if r.file_size_bytes > 0]

        return {
            "count": len(results),
            "duration_mean": round(statistics.mean(durations), 4),
            "duration_std": round(statistics.stdev(durations), 4)
            if len(durations) > 1
            else 0,
            "duration_min": round(min(durations), 4),
            "duration_max": round(max(durations), 4),
            "throughput_mean": round(statistics.mean(throughputs), 2),
            "throughput_max": round(max(throughputs), 2),
            "total_file_size_bytes": sum(file_sizes) if file_sizes else 0,
        }

    def get_comparison_table(self) -> List[Dict[str, Any]]:
        """Generate comparison data for reporting."""
        by_source = self.aggregate_by_source()

        table = []
        for source, stats in sorted(by_source.items()):
            table.append(
                {
                    "source": source,
                    "avg_duration_sec": stats.get("duration_mean", 0),
                    "avg_throughput_rows_sec": stats.get("throughput_mean", 0),
                    "runs": stats.get("count", 0),
                }
            )

        return sorted(table, key=lambda x: x["avg_throughput_rows_sec"], reverse=True)
