"""Minimal AUTO CDC pipeline that reproduces the spark-pipelines CLI RELTYPE_NOT_SET bug.
The @dp.table source and create_streaming_table register fine; create_auto_cdc_flow fails.

Run it:  spark-pipelines run --spec spark-pipeline.yml   (against the CLI's embedded server)
See BUG-spark-pipelines-autocdc-RELTYPE_NOT_SET.md for the full root-cause analysis.
"""
from typing import Any
from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark: Any  # injected by SDP

SCHEMA = StructType([
    StructField("id", IntegerType()), StructField("name", StringType()),
    StructField("city", StringType()), StructField("op", StringType()),
    StructField("seq", LongType()),
])

# NOTE: a file *glob* is required; a bare directory fails SDP analysis with PATH_NOT_FOUND.
CDC_GLOB = "file:///tmp/cli_repro/cdc/*.json"


@dp.table(name="cdc_src")
def cdc_src():
    return spark.readStream.schema(SCHEMA).json(CDC_GLOB)


dp.create_streaming_table("scd1")

dp.create_auto_cdc_flow(            # <-- fails here via the CLI with RELTYPE_NOT_SET
    target="scd1",
    source="cdc_src",
    keys=["id"],
    sequence_by="seq",
    stored_as_scd_type=1,
)
