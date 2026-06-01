"""Minimal AUTO CDC pipeline that reproduces the spark-pipelines CLI RELTYPE_NOT_SET bug.
The @dp.table source and create_streaming_table register fine; create_auto_cdc_flow fails."""
# --- deep proto probe (compare client/server Relation rel_type field numbers) ---
import sys
import pyspark
print("PROBE PYSPARK_FROM:", pyspark.__file__, file=sys.stderr)
print("PROBE PYSPARK_VERSION:", pyspark.__version__, file=sys.stderr)

from pyspark.sql.connect.proto import pipelines_pb2 as _probe_pb2
print("PROBE PB_FROM:", _probe_pb2.__file__, file=sys.stderr)
print(
    "PROBE PIPELINE_COMMAND_ONEOFS:",
    [o.name for o in _probe_pb2.PipelineCommand.DESCRIPTOR.oneofs],
    file=sys.stderr,
)
print(
    "PROBE DEFINE_FLOW_FIELDS:",
    [(f.number, f.name) for f in _probe_pb2.PipelineCommand.DefineFlow.DESCRIPTOR.fields],
    file=sys.stderr,
)
print(
    "PROBE DEFINE_FLOW_ONEOFS:",
    [
        (o.name, [f.name for f in o.fields])
        for o in _probe_pb2.PipelineCommand.DefineFlow.DESCRIPTOR.oneofs
    ],
    file=sys.stderr,
)
print("PROBE SYS_PATH_HEAD:", sys.path[:6], file=sys.stderr)

from pyspark.sql.connect.proto import relations_pb2 as _probe_rel
print("PROBE RELATIONS_PB_FROM:", _probe_rel.__file__, file=sys.stderr)
print(
    "PROBE RELATION_ONEOFS:",
    [o.name for o in _probe_rel.Relation.DESCRIPTOR.oneofs],
    file=sys.stderr,
)
_rel_type_oneof = next(
    o for o in _probe_rel.Relation.DESCRIPTOR.oneofs if o.name == "rel_type"
)
print(
    "PROBE RELATION_REL_TYPE_FIELDS_COUNT:",
    len(_rel_type_oneof.fields),
    file=sys.stderr,
)
print(
    "PROBE RELATION_REL_TYPE_FIELDS:",
    sorted([(f.number, f.name) for f in _rel_type_oneof.fields]),
    file=sys.stderr,
)

from pyspark.sql.connect.proto import relations_pb2 as _r
print("PROBE RELATIONS_PB_FROM:", _r.__file__)
print("PROBE RELATION_REL_TYPE_FIELDS_COUNT:",
      len(next(o for o in _r.Relation.DESCRIPTOR.oneofs if o.name == "rel_type").fields))
print("PROBE RELATION_REL_TYPE_FIELDS:",
      sorted([(f.number, f.name)
              for f in next(o for o in _r.Relation.DESCRIPTOR.oneofs if o.name == "rel_type").fields]))
# --- end deep proto probe ---
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
