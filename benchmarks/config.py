"""Benchmark configuration dataclasses."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone


class OperationType(Enum):
    """Types of operations to benchmark."""

    READ = "read"
    WRITE = "write"


class ProcessingMode(Enum):
    """Processing mode for benchmarks."""

    BATCH = "batch"
    STREAMING = "streaming"


class DataScale(Enum):
    """Predefined data scales for benchmarks."""

    TINY = 1_000  # 1K rows - quick validation
    SMALL = 10_000  # 10K rows - quick benchmarks
    MEDIUM = 100_000  # 100K rows - primary benchmark
    LARGE = 1_000_000  # 1M rows - full benchmark


@dataclass
class BenchmarkConfig:
    """Main benchmark configuration."""

    # Data scale
    row_counts: List[int] = field(
        default_factory=lambda: [
            DataScale.SMALL.value,
            DataScale.MEDIUM.value,
        ]
    )

    # Operations
    operations: List[OperationType] = field(
        default_factory=lambda: [
            OperationType.READ,
            OperationType.WRITE,
        ]
    )

    # Processing modes
    modes: List[ProcessingMode] = field(
        default_factory=lambda: [
            ProcessingMode.BATCH,
        ]
    )

    # Sources to benchmark
    sources: List[str] = field(
        default_factory=lambda: [
            "parquet",
            "csv",
            "json",
            "iceberg",
        ]
    )

    # Execution settings
    warmup_runs: int = 1
    benchmark_runs: int = 3
    seed: int = 42

    # Output settings
    output_dir: str = "benchmarks/results"
    output_format: str = "json"  # json, csv, or both

    # Spark settings
    spark_config: Dict[str, str] = field(
        default_factory=lambda: {
            "spark.sql.shuffle.partitions": "10",
            "spark.default.parallelism": "4",
        }
    )


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    source: str
    operation: OperationType
    mode: ProcessingMode
    row_count: int

    # Timing metrics (in seconds)
    duration_seconds: float
    warmup_duration_seconds: Optional[float] = None

    # Throughput metrics
    rows_per_second: float = 0.0
    mb_per_second: float = 0.0

    # Storage metrics
    file_size_bytes: int = 0
    compression_ratio: float = 1.0

    # Memory metrics (optional)
    peak_memory_mb: Optional[float] = None

    # Metadata
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    spark_version: str = ""
    run_id: str = ""

    # Statistical aggregates (populated when aggregating runs)
    duration_mean: Optional[float] = None
    duration_std: Optional[float] = None
    duration_min: Optional[float] = None
    duration_max: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source": self.source,
            "operation": self.operation.value,
            "mode": self.mode.value,
            "row_count": self.row_count,
            "duration_seconds": round(self.duration_seconds, 4),
            "rows_per_second": round(self.rows_per_second, 2),
            "mb_per_second": round(self.mb_per_second, 2),
            "file_size_bytes": self.file_size_bytes,
            "timestamp": self.timestamp.isoformat(),
            "spark_version": self.spark_version,
            "run_id": self.run_id,
        }
