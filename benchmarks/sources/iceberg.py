"""Iceberg table format benchmark."""

import os
from typing import Optional

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class IcebergBenchmark(SourceBenchmark):
    """Benchmark for Apache Iceberg table format."""

    source_name = "iceberg"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.catalog_name = "benchmark_catalog"
        self.namespace = "benchmark"
        self.warehouse_path = os.path.join(self.data_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)

    def setup_catalog(self) -> None:
        """Configure Spark session for Iceberg catalog."""
        # Configure Hadoop catalog (file-based, no external dependencies)
        self.spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.type",
            "hadoop",
        )
        self.spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.warehouse",
            self.warehouse_path,
        )

        # Create namespace if it doesn't exist
        try:
            self.spark.sql(
                f"CREATE NAMESPACE IF NOT EXISTS {self.catalog_name}.{self.namespace}"
            )
        except Exception:
            # Namespace might already exist
            pass

    def get_table_name(self, name: str) -> str:
        """Get fully qualified table name."""
        # Clean table name for SQL
        clean_name = name.replace("-", "_").replace(".", "_")
        return f"{self.catalog_name}.{self.namespace}.{clean_name}"

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        table_name = self.get_table_name(name)
        df.writeTo(table_name).using("iceberg").createOrReplace()

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        table_name = self.get_table_name(name)
        return self.spark.table(table_name)

    def data_exists(self, name: str) -> bool:
        """Check if Iceberg table exists."""
        table_name = self.get_table_name(name)
        try:
            self.spark.table(table_name)
            return True
        except Exception:
            return False

    def get_path(self, name: str) -> str:
        """Get warehouse path for size calculation."""
        clean_name = name.replace("-", "_").replace(".", "_")
        return os.path.join(self.warehouse_path, self.namespace, clean_name)

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write to Iceberg table."""
        # Extract table name from path
        name = os.path.basename(path)
        table_name = self.get_table_name(name)
        df.writeTo(table_name).using("iceberg").createOrReplace()

    def _timed_read(self, path: str) -> DataFrame:
        """Read from Iceberg table."""
        name = os.path.basename(path)
        table_name = self.get_table_name(name)
        return self.spark.table(table_name)

    def benchmark_time_travel(
        self,
        name: str,
        row_count: int,
        snapshot_id: Optional[int] = None,
    ) -> Optional[dict]:
        """Benchmark time travel read (returns timing info)."""
        from ..core.metrics import TimingContext

        table_name = self.get_table_name(name)

        # Get available snapshots
        try:
            snapshots_df = self.spark.sql(
                f"SELECT snapshot_id, committed_at FROM {table_name}.snapshots "
                "ORDER BY committed_at DESC LIMIT 5"
            )
            snapshots = snapshots_df.collect()
            if not snapshots:
                return None

            # Use specified or oldest snapshot
            target_snapshot = snapshot_id or snapshots[-1]["snapshot_id"]

            with TimingContext() as timer:
                df = self.spark.read.option("snapshot-id", target_snapshot).table(
                    table_name
                )
                df.count()

            return {
                "snapshot_id": target_snapshot,
                "duration_seconds": timer.duration,
                "rows_per_second": row_count / timer.duration if timer.duration > 0 else 0,
            }
        except Exception as e:
            print(f"Time travel benchmark failed: {e}")
            return None
