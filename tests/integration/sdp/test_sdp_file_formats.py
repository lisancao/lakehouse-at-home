"""SDP coverage tests for file formats.

Tests: Parquet, JSON, CSV, ORC, Avro, Text
"""

import os
import pytest
from pyspark.sql import functions as f


pytestmark = [pytest.mark.sdp, pytest.mark.file_formats]


class TestParquetFormat:
    """Parquet file format tests."""

    def test_write_parquet(self, spark, sample_df, temp_data_dir):
        """Test writing Parquet files."""
        path = os.path.join(temp_data_dir, "test.parquet")
        sample_df.write.mode("overwrite").parquet(path)
        assert os.path.exists(path)

    def test_read_parquet(self, spark, sample_df, temp_data_dir):
        """Test reading Parquet files."""
        path = os.path.join(temp_data_dir, "read_test.parquet")
        sample_df.write.mode("overwrite").parquet(path)

        df = spark.read.parquet(path)
        assert df.count() == 5
        assert set(df.columns) == {"id", "name", "value"}

    def test_parquet_schema_inference(self, spark, sample_df, temp_data_dir):
        """Test Parquet schema columns and types are preserved."""
        path = os.path.join(temp_data_dir, "schema_test.parquet")
        sample_df.write.mode("overwrite").parquet(path)

        df = spark.read.parquet(path)
        # Check column names and types match (nullable may differ)
        assert [f.name for f in df.schema.fields] == [f.name for f in sample_df.schema.fields]
        assert [f.dataType for f in df.schema.fields] == [f.dataType for f in sample_df.schema.fields]

    def test_parquet_compression(self, spark, sample_df, temp_data_dir):
        """Test Parquet compression options."""
        for compression in ["snappy", "gzip", "zstd"]:
            path = os.path.join(temp_data_dir, f"compress_{compression}.parquet")
            sample_df.write.mode("overwrite").option(
                "compression", compression
            ).parquet(path)
            assert os.path.exists(path)

            # Verify can read back
            df = spark.read.parquet(path)
            assert df.count() == 5

    def test_parquet_partitioned_write(self, spark, temp_data_dir):
        """Test partitioned Parquet writes."""
        path = os.path.join(temp_data_dir, "partitioned.parquet")

        df = spark.createDataFrame([
            (1, "a", 100),
            (2, "a", 200),
            (3, "b", 300),
            (4, "b", 400),
        ], ["id", "category", "value"])

        df.write.mode("overwrite").partitionBy("category").parquet(path)

        # Verify partitions exist
        assert os.path.exists(os.path.join(path, "category=a"))
        assert os.path.exists(os.path.join(path, "category=b"))


class TestJSONFormat:
    """JSON file format tests."""

    def test_write_json(self, spark, sample_df, temp_data_dir):
        """Test writing JSON files."""
        path = os.path.join(temp_data_dir, "test.json")
        sample_df.write.mode("overwrite").json(path)
        assert os.path.exists(path)

    def test_read_json(self, spark, sample_df, temp_data_dir):
        """Test reading JSON files."""
        path = os.path.join(temp_data_dir, "read_test.json")
        sample_df.write.mode("overwrite").json(path)

        df = spark.read.json(path)
        assert df.count() == 5

    def test_json_multiline(self, spark, temp_data_dir):
        """Test multiline JSON reading."""
        import json

        path = os.path.join(temp_data_dir, "multiline.json")

        # Write multiline JSON
        data = [{"id": 1, "name": "test", "nested": {"key": "value"}}]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

        df = spark.read.option("multiline", "true").json(path)
        assert df.count() == 1

    def test_json_schema_inference(self, spark, sample_df, temp_data_dir):
        """Test JSON schema inference."""
        path = os.path.join(temp_data_dir, "schema_test.json")
        sample_df.write.mode("overwrite").json(path)

        df = spark.read.json(path)
        # JSON infers types, verify columns exist
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns


class TestCSVFormat:
    """CSV file format tests."""

    def test_write_csv(self, spark, sample_df, temp_data_dir):
        """Test writing CSV files."""
        path = os.path.join(temp_data_dir, "test.csv")
        sample_df.write.mode("overwrite").option("header", "true").csv(path)
        assert os.path.exists(path)

    def test_read_csv_with_header(self, spark, sample_df, temp_data_dir):
        """Test reading CSV with header."""
        path = os.path.join(temp_data_dir, "header_test.csv")
        sample_df.write.mode("overwrite").option("header", "true").csv(path)

        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        assert df.count() == 5
        assert set(df.columns) == {"id", "name", "value"}

    def test_csv_with_explicit_schema(self, spark, sample_df, temp_data_dir):
        """Test reading CSV with explicit schema."""
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType

        path = os.path.join(temp_data_dir, "schema_test.csv")
        sample_df.write.mode("overwrite").option("header", "true").csv(path)

        schema = StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", IntegerType()),
        ])

        df = spark.read.option("header", "true").schema(schema).csv(path)
        assert df.count() == 5
        assert df.schema == schema

    def test_csv_delimiter(self, spark, sample_df, temp_data_dir):
        """Test CSV with custom delimiter."""
        path = os.path.join(temp_data_dir, "tab_delim.csv")
        sample_df.write.mode("overwrite").option("header", "true").option(
            "delimiter", "\t"
        ).csv(path)

        df = spark.read.option("header", "true").option("delimiter", "\t").option(
            "inferSchema", "true"
        ).csv(path)
        assert df.count() == 5


class TestORCFormat:
    """ORC file format tests."""

    def test_write_orc(self, spark, sample_df, temp_data_dir):
        """Test writing ORC files."""
        path = os.path.join(temp_data_dir, "test.orc")
        sample_df.write.mode("overwrite").orc(path)
        assert os.path.exists(path)

    def test_read_orc(self, spark, sample_df, temp_data_dir):
        """Test reading ORC files."""
        path = os.path.join(temp_data_dir, "read_test.orc")
        sample_df.write.mode("overwrite").orc(path)

        df = spark.read.orc(path)
        assert df.count() == 5
        assert set(df.columns) == {"id", "name", "value"}

    def test_orc_compression(self, spark, sample_df, temp_data_dir):
        """Test ORC compression options."""
        for compression in ["snappy", "zlib"]:
            path = os.path.join(temp_data_dir, f"compress_{compression}.orc")
            sample_df.write.mode("overwrite").option(
                "compression", compression
            ).orc(path)
            assert os.path.exists(path)

    def test_orc_schema_preserved(self, spark, sample_df, temp_data_dir):
        """Test ORC schema columns and types are preserved."""
        path = os.path.join(temp_data_dir, "schema_test.orc")
        sample_df.write.mode("overwrite").orc(path)

        df = spark.read.orc(path)
        # Check column names and types match (nullable may differ)
        assert [f.name for f in df.schema.fields] == [f.name for f in sample_df.schema.fields]
        assert [f.dataType for f in df.schema.fields] == [f.dataType for f in sample_df.schema.fields]


class TestTextFormat:
    """Text file format tests."""

    def test_write_text(self, spark, temp_data_dir):
        """Test writing text files."""
        path = os.path.join(temp_data_dir, "test.txt")
        df = spark.createDataFrame([("line1",), ("line2",), ("line3",)], ["value"])
        df.write.mode("overwrite").text(path)
        assert os.path.exists(path)

    def test_read_text(self, spark, temp_data_dir):
        """Test reading text files."""
        path = os.path.join(temp_data_dir, "read_test.txt")
        df = spark.createDataFrame(
            [("hello world",), ("goodbye world",)], ["value"]
        )
        df.write.mode("overwrite").text(path)

        result = spark.read.text(path)
        assert result.count() == 2
        assert "value" in result.columns


class TestGenericDataSource:
    """Test generic .format() API."""

    def test_format_parquet(self, spark, sample_df, temp_data_dir):
        """Test using .format() for Parquet."""
        path = os.path.join(temp_data_dir, "format.parquet")
        sample_df.write.format("parquet").mode("overwrite").save(path)

        df = spark.read.format("parquet").load(path)
        assert df.count() == 5

    def test_format_json(self, spark, sample_df, temp_data_dir):
        """Test using .format() for JSON."""
        path = os.path.join(temp_data_dir, "format.json")
        sample_df.write.format("json").mode("overwrite").save(path)

        df = spark.read.format("json").load(path)
        assert df.count() == 5

    def test_format_csv(self, spark, sample_df, temp_data_dir):
        """Test using .format() for CSV."""
        path = os.path.join(temp_data_dir, "format.csv")
        sample_df.write.format("csv").option("header", "true").mode("overwrite").save(
            path
        )

        df = spark.read.format("csv").option("header", "true").load(path)
        assert df.count() == 5

    def test_format_orc(self, spark, sample_df, temp_data_dir):
        """Test using .format() for ORC."""
        path = os.path.join(temp_data_dir, "format.orc")
        sample_df.write.format("orc").mode("overwrite").save(path)

        df = spark.read.format("orc").load(path)
        assert df.count() == 5
