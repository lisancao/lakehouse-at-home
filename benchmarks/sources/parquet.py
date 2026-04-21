"""Parquet format benchmark."""

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class ParquetBenchmark(SourceBenchmark):
    """Benchmark for Parquet file format."""

    source_name = "parquet"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        path = self.get_path(name)
        df.write.mode("overwrite").parquet(path)

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        path = self.get_path(name)
        return self.spark.read.parquet(path)

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write Parquet with default compression (snappy)."""
        df.write.mode("overwrite").parquet(path)

    def _timed_read(self, path: str) -> DataFrame:
        """Read Parquet file."""
        return self.spark.read.parquet(path)
