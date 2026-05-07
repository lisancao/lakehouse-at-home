# Airflow 3.1.6 + Spark 4.1 + patched apache-spark provider.
#
# We extend a previously-built local image (lakehouse-stack/airflow-base:3.1.6)
# rather than rebuilding from apache/airflow because the original Dockerfile
# downloads spark-4.1.0-bin-hadoop3.tgz from archive.apache.org, which is
# rate-limited to ~70 KB/s and would take 2+ hours. The base image already
# carries: openjdk-17, /opt/spark (Spark 4.1.0), JAVA_HOME, SPARK_HOME, the
# original provider set, and pyspark==4.1.0.
#
# To rebuild the base from scratch (rare), use Dockerfile.base which still
# pulls Spark from archive.apache.org. For day-to-day development, this thin
# top layer is what changes.
FROM lakehouse-stack/airflow-base:3.1.6

# --- begin patched layer ---

USER airflow

# Vendored apache-spark provider — patched fork at lisancao/airflow
# fix/spark-pipelines-connect-mode that fixes SparkPipelinesHook to honor
# spark_connect-typed connections (sets SPARK_REMOTE, skips spark-submit
# cluster-manager flags, invokes pyspark.pipelines.cli directly).
# Drop this COPY + use `apache-airflow-providers-apache-spark>=6.x` once the
# fix lands upstream.
COPY --chown=airflow:0 docker/airflow/wheels/apache_airflow_providers_apache_spark-6.0.1-py3-none-any.whl /tmp/

# Install the patched provider AND the Connect-CLI Python deps that the
# upstream apache/spark image is missing (pyarrow, pandas>=2.2, pyyaml,
# grpcio, zstandard). --force-reinstall ensures the patched provider wins
# over whatever version the base image has.
RUN pip install --no-cache-dir --force-reinstall --no-deps \
        /tmp/apache_airflow_providers_apache_spark-6.0.1-py3-none-any.whl \
    && pip install --no-cache-dir \
        'pandas>=2.2' \
        'pyarrow>=15' \
        'grpcio>=1.62' \
        'grpcio-status>=1.62' \
        googleapis-common-protos \
        'protobuf>=5.27' \
        'zstandard>=0.25' \
        pyyaml \
        'apache-airflow-providers-keycloak>=0.7' \
        'apache-airflow-providers-openlineage>=2.6' \
    && rm /tmp/apache_airflow_providers_apache_spark-6.0.1-py3-none-any.whl

WORKDIR /opt/airflow
