"""Smoke tests for the UDF performance benchmark.

Uses the session-scoped ``spark`` fixture from conftest.py (local[2]).
Runs with TINY scale (1 000 rows) to verify UDF types produce correct
results and that timing is recorded.

Arrow UDFs are skipped in classic mode (codegen limitation in Spark 4.1).
Pandas UDFs require pyarrow on the worker — the fixture sets PYSPARK_PYTHON
to ensure the venv interpreter is used.
"""

import os
import sys

import pytest
from pyspark.sql import functions as F

from benchmarks.pipelines.udf_benchmark import UdfBenchmarkResult, UdfPipelineBenchmark


TINY_ROWS = 1_000


@pytest.fixture(scope="module", autouse=True)
def _set_pyspark_python():
    """Ensure Spark workers use the same Python as the driver."""
    orig = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    yield
    if orig is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = orig


@pytest.fixture(scope="module")
def benchmark(spark):
    """Create a benchmark instance with minimal iteration counts."""
    return UdfPipelineBenchmark(spark, warmup_runs=0, benchmark_runs=1)


@pytest.fixture(scope="module")
def test_df(benchmark):
    """Small cached DataFrame for UDF tests."""
    df = benchmark.generate_test_data(TINY_ROWS)
    df.cache()
    df.count()
    return df


# ------------------------------------------------------------------
# Data generation
# ------------------------------------------------------------------

class TestDataGeneration:

    def test_row_count(self, test_df):
        assert test_df.count() == TINY_ROWS

    def test_columns_present(self, test_df):
        assert set(test_df.columns) == {"id", "value", "name"}

    def test_value_range(self, test_df):
        stats = test_df.agg(
            F.min("value").alias("min_v"),
            F.max("value").alias("max_v"),
        ).collect()[0]
        assert stats.min_v >= 0.0
        assert stats.max_v < 10.0  # (id % 1000) / 100

    def test_name_format(self, test_df):
        sample = test_df.select("name").limit(5).collect()
        for row in sample:
            assert row.name.startswith("item_")


# ------------------------------------------------------------------
# Built-in functions (baseline)
# ------------------------------------------------------------------

class TestBuiltinUDFs:

    def test_arithmetic(self, benchmark, test_df):
        result = benchmark._builtin_arithmetic(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        # value for id=0 is 0.0; 0*2+1 = 1.0
        assert row.result == pytest.approx(1.0)

    def test_string(self, benchmark, test_df):
        result = benchmark._builtin_string(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, benchmark, test_df):
        result = benchmark._builtin_cdf(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        # CDF(0) = 0.5
        assert row.result == pytest.approx(0.5, abs=0.01)


# ------------------------------------------------------------------
# SQL UDFs
# ------------------------------------------------------------------

class TestSqlUDFs:

    def test_arithmetic(self, benchmark, test_df):
        benchmark._register_sql_udfs()
        result = benchmark._sql_udf_arithmetic(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, benchmark, test_df):
        benchmark._register_sql_udfs()
        result = benchmark._sql_udf_string(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"


# ------------------------------------------------------------------
# Arrow UDFs (Spark 4.1)
# ------------------------------------------------------------------

@pytest.mark.skip(reason="arrow_udf codegen not supported in classic mode (Spark 4.1)")
class TestArrowUDFs:
    """Arrow UDFs require Spark Connect or a future codegen fix."""

    @pytest.fixture(scope="class")
    def arrow_udfs(self):
        return UdfPipelineBenchmark._make_arrow_udfs()

    def test_arithmetic(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert "_SUFFIX" in row.result

    def test_cdf(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


# ------------------------------------------------------------------
# Pandas UDFs
# ------------------------------------------------------------------

class TestPandasUDFs:

    @pytest.fixture(scope="class")
    def pandas_udfs(self):
        return UdfPipelineBenchmark._make_pandas_udfs()

    def test_arithmetic(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


# ------------------------------------------------------------------
# Python UDFs (arrow-opt and pickle share the same functions)
# ------------------------------------------------------------------

class TestPythonUDFs:

    @pytest.fixture(scope="class")
    def python_udfs(self):
        return UdfPipelineBenchmark._make_python_udfs()

    def test_arithmetic(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


# ------------------------------------------------------------------
# Full comparison (integration)
# ------------------------------------------------------------------

class TestRunComparison:

    def test_run_comparison_returns_results(self, benchmark):
        results = benchmark.run_comparison(row_count=TINY_ROWS)

        # At minimum: 3 builtin + 2 sql_udf + 3 pandas + 3 pickle = 11
        # arrow_udf (3) and arrow_opt_python (3) may error in classic mode
        assert len(results) >= 11

        for _key, result in results.items():
            assert isinstance(result, UdfBenchmarkResult)
            assert result.row_count == TINY_ROWS
            assert result.duration_seconds > 0
            assert result.rows_per_second > 0
            assert result.run_id == benchmark.run_id

    def test_baseline_overhead_is_one(self, benchmark):
        results = benchmark.run_comparison(row_count=TINY_ROWS)
        for _key, result in results.items():
            if result.udf_type == "builtin":
                assert result.relative_overhead == pytest.approx(1.0)
