# MLflow 3.1 tracking server + AI Gateway.
#
# The upstream ghcr.io/mlflow/mlflow image does not include psycopg2, so
# pointing MLFLOW_BACKEND_STORE_URI at PostgreSQL crashes on boot. Layer
# psycopg2-binary and boto3 on so the image can reach both the shared
# Postgres catalog and the SeaweedFS S3 endpoint for artifacts.
FROM ghcr.io/mlflow/mlflow:v3.1.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
      psycopg2-binary==2.9.9 \
      boto3==1.35.0

COPY docker/mlflow/entrypoint.sh /usr/local/bin/mlflow-entrypoint.sh
RUN chmod +x /usr/local/bin/mlflow-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/mlflow-entrypoint.sh"]
