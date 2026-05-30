# Apache Spark 5.0.0-SNAPSHOT (master, source-built) + Iceberg runtime ported to
# Spark 5.0 — for testing AUTO CDC / SCD Type 1 on the lakehouse stack.
#
# Build context must contain:
#   spark.tgz           - the Spark dist built with: make-distribution.sh --pip --tgz --connect -Phive
#   iceberg-runtime.jar - the Iceberg runtime jar ported to Spark 5.0 (see scripts/pipelines/autocdc-scd1/iceberg-port/)
#
# Built/wired by docker-compose-spark42.yml.
FROM eclipse-temurin:17-jammy

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip procps bash tini libc6 \
 && rm -rf /var/lib/apt/lists/*

COPY spark.tgz /tmp/spark.tgz
RUN mkdir -p /opt/spark \
 && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
 && rm /tmp/spark.tgz

# Iceberg runtime (Spark-5.0 port) on the default jars path so SparkCatalog resolves.
COPY iceberg-runtime.jar /opt/spark/jars/iceberg-spark-runtime.jar

# Spark Connect / declarative-pipelines client deps.
RUN pip3 install --no-cache-dir \
      pyarrow "pandas<3.0.0" numpy grpcio grpcio-status \
      googleapis-common-protos "zstandard>=0.25.0" pyyaml

WORKDIR /opt/spark
