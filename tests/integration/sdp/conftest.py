"""Pytest fixtures for SDP data source tests.

Provides Spark sessions configured for different data source types.
"""

import pytest
import tempfile
import shutil
from typing import Generator, Optional

from pyspark.sql import SparkSession


def pytest_configure(config):
    """Register SDP-specific markers."""
    config.addinivalue_line("markers", "sdp: mark test as SDP data source test")
    config.addinivalue_line("markers", "file_formats: mark test for file format sources")
    config.addinivalue_line("markers", "table_formats: mark test for table format sources")
    config.addinivalue_line("markers", "streaming: mark test for streaming sources")
    config.addinivalue_line("markers", "benchmark: mark test as benchmark (may be slow)")


@pytest.fixture(scope="session")
def spark_with_iceberg() -> Generator[SparkSession, None, None]:
    """Create SparkSession with Iceberg support (Hadoop catalog)."""
    warehouse = tempfile.mkdtemp(prefix="iceberg_test_")

    spark = (
        SparkSession.builder.appName("sdp-iceberg-tests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Iceberg configuration
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.test_iceberg.type", "hadoop")
        .config("spark.sql.catalog.test_iceberg.warehouse", warehouse)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Create test namespace
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test_iceberg.sdp_tests")

    yield spark

    spark.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def spark_with_delta() -> Generator[Optional[SparkSession], None, None]:
    """Create SparkSession with Delta Lake support."""
    warehouse = tempfile.mkdtemp(prefix="delta_test_")
    spark: Optional[SparkSession] = None

    try:
        spark = (
            SparkSession.builder.appName("sdp-delta-tests")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            # Delta configuration
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

        yield spark

    except Exception as e:
        pytest.skip(f"Delta Lake not available: {e}")
        yield None
    finally:
        if spark:
            spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="function")
def temp_data_dir(tmp_path) -> str:
    """Provide a temporary directory for test data."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir)


@pytest.fixture(scope="session")
def sample_df(spark):
    """Create a sample DataFrame for testing."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
    ])

    data = [
        (1, "alpha", 100),
        (2, "beta", 200),
        (3, "gamma", 300),
        (4, "delta", 400),
        (5, "epsilon", 500),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def events_df(spark):
    """Create a sample events DataFrame for streaming tests."""
    from pyspark.sql.types import StructType, StructField, StringType, LongType

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("payload", StringType(), True),
    ])

    data = [
        ("evt001", "order_created", 1704067200, '{"total": 25.99}'),
        ("evt002", "order_started", 1704067260, '{"kitchen_id": 1}'),
        ("evt003", "order_ready", 1704067500, '{"ready": true}'),
        ("evt004", "order_delivered", 1704067800, '{"driver_id": 42}'),
        ("evt005", "order_created", 1704068100, '{"total": 15.50}'),
    ]

    return spark.createDataFrame(data, schema)
