"""ORC format benchmark."""

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class ORCBenchmark(SourceBenchmark):
    """Benchmark for ORC file format."""

    source_name = "orc"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        path = self.get_path(name)
        df.write.mode("overwrite").orc(path)

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        path = self.get_path(name)
        return self.spark.read.orc(path)

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write ORC with default compression (zlib)."""
        df.write.mode("overwrite").orc(path)

    def _timed_read(self, path: str) -> DataFrame:
        """Read ORC file."""
        return self.spark.read.orc(path)
