"""JSON format benchmark."""

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class JSONBenchmark(SourceBenchmark):
    """Benchmark for JSON file format."""

    source_name = "json"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        path = self.get_path(name)
        df.write.mode("overwrite").json(path)

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        path = self.get_path(name)
        return self.spark.read.json(path)

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write JSON lines format."""
        df.write.mode("overwrite").json(path)

    def _timed_read(self, path: str) -> DataFrame:
        """Read JSON with schema inference."""
        return self.spark.read.json(path)
