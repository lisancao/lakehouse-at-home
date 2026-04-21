"""CSV format benchmark."""

from pyspark.sql import DataFrame

from ..config import OperationType
from .base import SourceBenchmark


class CSVBenchmark(SourceBenchmark):
    """Benchmark for CSV file format."""

    source_name = "csv"
    supported_operations = [OperationType.READ, OperationType.WRITE]

    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing."""
        path = self.get_path(name)
        df.write.mode("overwrite").option("header", "true").csv(path)

    def read_data(self, name: str) -> DataFrame:
        """Read data without timing."""
        path = self.get_path(name)
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(
            path
        )

    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Write CSV with header."""
        df.write.mode("overwrite").option("header", "true").csv(path)

    def _timed_read(self, path: str) -> DataFrame:
        """Read CSV with header and schema inference."""
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(
            path
        )
