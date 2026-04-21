"""Pipeline benchmark runner."""

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Any

from pyspark.sql import SparkSession

from .kafka_benchmark import KafkaPipelineBenchmark, PipelineResult


class PipelineBenchmarkRunner:
    """Run and compare pipeline benchmarks."""

    def __init__(
        self,
        spark: SparkSession,
        output_dir: str = "benchmarks/results/pipelines",
    ):
        self.spark = spark
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def run_kafka_benchmark(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        topic: str = "benchmark_orders",
        message_count: int = 1000,
        duration_seconds: int = 30,
        trigger_interval: str = "5 seconds",
    ) -> Dict[str, PipelineResult]:
        """Run Kafka pipeline benchmark."""
        benchmark = KafkaPipelineBenchmark(
            spark=self.spark,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
        )

        try:
            results = benchmark.run_comparison(
                message_count=message_count,
                duration_seconds=duration_seconds,
                trigger_interval=trigger_interval,
            )

            # Save results
            self._save_results(results, benchmark.run_id)

            return results
        finally:
            benchmark.cleanup()

    def _save_results(
        self,
        results: Dict[str, PipelineResult],
        run_id: str,
    ) -> str:
        """Save benchmark results to JSON."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"pipeline_benchmark_{run_id}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        data = {
            "metadata": {
                "run_id": run_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "benchmark_type": "pipeline_comparison",
            },
            "results": {k: v.to_dict() for k, v in results.items()},
            "comparison": self._compute_comparison(results),
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        print(f"\nResults saved to: {filepath}")
        return filepath

    def _compute_comparison(
        self,
        results: Dict[str, PipelineResult],
    ) -> Dict[str, Any]:
        """Compute comparison metrics."""
        imp = results.get("imperative")
        dec = results.get("declarative")

        if not imp or not dec:
            return {}

        throughput_ratio = (
            imp.throughput_msg_per_sec / dec.throughput_msg_per_sec
            if dec.throughput_msg_per_sec > 0 else 0
        )

        latency_ratio = (
            imp.avg_latency_ms / dec.avg_latency_ms
            if dec.avg_latency_ms > 0 else 0
        )

        return {
            "throughput_ratio_imp_vs_dec": round(throughput_ratio, 3),
            "latency_ratio_imp_vs_dec": round(latency_ratio, 3),
            "winner_throughput": "imperative" if throughput_ratio > 1 else "declarative",
            "winner_latency": "declarative" if latency_ratio > 1 else "imperative",
        }
