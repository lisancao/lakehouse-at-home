"""Lakehouse Benchmark Suite.

Benchmark framework for comparing data source performance in Spark Declarative Pipelines.

Usage:
    python -m benchmarks run --sources parquet,iceberg --scales 10000,100000
    python -m benchmarks compare results1.json results2.json
"""

__version__ = "0.1.0"
