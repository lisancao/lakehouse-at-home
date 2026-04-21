"""Delta Lake table format benchmark."""

import os
from typing import Optional

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class DeltaBenchmark(SourceBenchmark):
    """Benchmark for Delta Lake table format."""

    source_name = "delta"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._delta_configured = False

    def setup_delta(self) -> None:
        """Verify Delta Lake is available."""
        if self._delta_configured:
            return

        # Check if Delta extensions are configured
        try:
            # Try to use Delta - will fail if JARs not available
            test_path = os.path.join(self.data_dir, "_delta_test")
            test_df = self.spark.range(1)
            test_df.write.format("delta").mode("overwrite").save(test_path)
            self.spark.read.format("delta").load(test_path)
            self._delta_configured = True

            # Cleanup test
            import shutil

            shutil.rmtree(test_path, ignore_errors=True)
        except Exception as e:
            raise RuntimeError(
                f"Delta Lake not configured properly. "
                f"Ensure delta-spark JAR is available: {e}"
            )

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        path = self.get_path(name)
        df.write.format("delta").mode("overwrite").save(path)

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        path = self.get_path(name)
        return self.spark.read.format("delta").load(path)

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write Delta table."""
        df.write.format("delta").mode("overwrite").save(path)

    def _timed_read(self, path: str) -> DataFrame:
        """Read Delta table."""
        return self.spark.read.format("delta").load(path)

    def benchmark_time_travel(
        self,
        name: str,
        row_count: int,
        version: Optional[int] = None,
    ) -> Optional[dict]:
        """Benchmark time travel read by version."""
        from ..core.metrics import TimingContext

        path = self.get_path(name)

        try:
            # Get available versions
            history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
            versions = [row["version"] for row in history_df.collect()]

            if not versions:
                return None

            # Use specified or oldest version
            target_version = version if version is not None else min(versions)

            with TimingContext() as timer:
                df = self.spark.read.format("delta").option(
                    "versionAsOf", target_version
                ).load(path)
                df.count()

            return {
                "version": target_version,
                "duration_seconds": timer.duration,
                "rows_per_second": row_count / timer.duration if timer.duration > 0 else 0,
            }
        except Exception as e:
            print(f"Time travel benchmark failed: {e}")
            return None

    def benchmark_time_travel_timestamp(
        self,
        name: str,
        row_count: int,
        timestamp: Optional[str] = None,
    ) -> Optional[dict]:
        """Benchmark time travel read by timestamp."""
        from ..core.metrics import TimingContext

        path = self.get_path(name)

        try:
            # If no timestamp, use earliest available
            if not timestamp:
                history_df = self.spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
                timestamps = [
                    row["timestamp"].isoformat() for row in history_df.collect()
                ]
                if not timestamps:
                    return None
                timestamp = timestamps[-1]  # Oldest

            with TimingContext() as timer:
                df = self.spark.read.format("delta").option(
                    "timestampAsOf", timestamp
                ).load(path)
                df.count()

            return {
                "timestamp": timestamp,
                "duration_seconds": timer.duration,
                "rows_per_second": row_count / timer.duration if timer.duration > 0 else 0,
            }
        except Exception as e:
            print(f"Timestamp travel benchmark failed: {e}")
            return None
