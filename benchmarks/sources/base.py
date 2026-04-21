"""Abstract base class for source benchmarks."""

import os
from abc import ABC, abstractmethod
from typing import List

from pyspark.sql import SparkSession, DataFrame

from ..config import BenchmarkConfig, BenchmarkResult, OperationType, ProcessingMode
from ..core.metrics import TimingContext


class SourceBenchmark(ABC):
    """Base class for all source benchmarks."""

    # Subclasses should define these
    source_name: str = "unknown"
    supported_operations: List[OperationType] = [OperationType.READ, OperationType.WRITE]

    def __init__(
        self,
        spark: SparkSession,
        config: BenchmarkConfig,
        temp_dir: str,
    ):
        self.spark = spark
        self.config = config
        self.temp_dir = temp_dir
        self.data_dir = os.path.join(temp_dir, self.source_name)
        os.makedirs(self.data_dir, exist_ok=True)

    def get_path(self, name: str) -> str:
        """Get full path for a data file/table."""
        return os.path.join(self.data_dir, name)

    def data_exists(self, name: str) -> bool:
        """Check if data exists at the given path."""
        path = self.get_path(name)
        return os.path.exists(path)

    @abstractmethod
    def write_data(self, df: DataFrame, name: str) -> None:
        """Write data without timing (for setup)."""
        pass

    @abstractmethod
    def read_data(self, name: str) -> DataFrame:
        """Read data without timing (for validation)."""
        pass

    def benchmark_write(
        self,
        df: DataFrame,
        name: str,
        row_count: int,
    ) -> BenchmarkResult:
        """Benchmark a write operation."""
        path = self.get_path(name)

        with TimingContext() as timer:
            self._timed_write(df, path)

        file_size = self._get_file_size(path)

        return BenchmarkResult(
            source=self.source_name,
            operation=OperationType.WRITE,
            mode=ProcessingMode.BATCH,
            row_count=row_count,
            duration_seconds=timer.duration,
            rows_per_second=row_count / timer.duration if timer.duration > 0 else 0,
            mb_per_second=(file_size / (1024 * 1024)) / timer.duration
            if timer.duration > 0
            else 0,
            file_size_bytes=file_size,
        )

    def benchmark_read(
        self,
        name: str,
        row_count: int,
    ) -> BenchmarkResult:
        """Benchmark a read operation."""
        path = self.get_path(name)
        file_size = self._get_file_size(path)

        with TimingContext() as timer:
            df = self._timed_read(path)
            # Force materialization
            df.count()

        return BenchmarkResult(
            source=self.source_name,
            operation=OperationType.READ,
            mode=ProcessingMode.BATCH,
            row_count=row_count,
            duration_seconds=timer.duration,
            rows_per_second=row_count / timer.duration if timer.duration > 0 else 0,
            mb_per_second=(file_size / (1024 * 1024)) / timer.duration
            if timer.duration > 0
            else 0,
            file_size_bytes=file_size,
        )

    @abstractmethod
    def _timed_write(self, df: DataFrame, path: str) -> None:
        """Perform the actual write operation (timed)."""
        pass

    @abstractmethod
    def _timed_read(self, path: str) -> DataFrame:
        """Perform the actual read operation (timed)."""
        pass

    def _get_file_size(self, path: str) -> int:
        """Get total size of files at path."""
        if not os.path.exists(path):
            return 0

        if os.path.isfile(path):
            return os.path.getsize(path)

        total_size = 0
        for dirpath, _, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total_size += os.path.getsize(filepath)
        return total_size

    def cleanup(self, name: str) -> None:
        """Clean up data at the given path."""
        import shutil

        path = self.get_path(name)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
