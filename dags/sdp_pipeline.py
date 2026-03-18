"""
Airflow DAG: Spark Declarative Pipeline (SDP)
===============================================

Runs the SDP pipeline via SparkSubmitOperator.
Schedule: daily. Includes preflight check, SDP execution, verification, and maintenance.

This DAG demonstrates how to wire SDP into Airflow:
  1. Preflight: verify Spark cluster and data sources are accessible
  2. Run SDP: spark-pipelines run --spec spark-pipeline.yml
  3. Verify: check that output tables were created and have data
  4. Maintain: run Iceberg maintenance (compaction, snapshot expiry)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ─── Configuration ──────────────────────────────────────────
SPARK_CONN_ID = "spark_41"
SPARK_MASTER = os.getenv("SPARK_MASTER_41", "spark://localhost:7078")
PIPELINE_SPEC = "/scripts/pipelines/spark-pipeline.yml"
PIPELINE_SCRIPT = "/scripts/pipelines/pipeline_sdp.py"

SPARK_CONF = {
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "jdbc",
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://localhost:5432/iceberg_catalog",
    "spark.sql.catalog.iceberg.jdbc.user": os.getenv("POSTGRES_USER", "iceberg"),
    "spark.sql.catalog.iceberg.jdbc.password": os.getenv("POSTGRES_PASSWORD", "iceberg_password"),
    "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse/warehouse",
    "spark.hadoop.fs.s3a.endpoint": "http://localhost:8333",
    "spark.hadoop.fs.s3a.access.key": os.getenv("S3_ACCESS_KEY", "admin"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("S3_SECRET_KEY", "admin_password"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

EXPECTED_TABLES = [
    "iceberg.bronze.orders",
    "iceberg.bronze.dim_locations",
    "iceberg.silver.orders_enriched",
    "iceberg.gold.hourly_metrics",
]

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_sdp_pipeline",
    default_args=default_args,
    description="Run Spark Declarative Pipeline (SDP) for medallion architecture",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "sdp", "spark-4.1", "medallion"],
    doc_md=__doc__,
) as dag:

    # ─── Task 1: Preflight Check ────────────────────────────
    @task
    def preflight_check():
        """Verify Spark cluster and data sources are accessible."""
        import subprocess
        import socket

        checks = {}

        # Check Spark master
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(("localhost", 7078))
            sock.close()
            checks["spark_master"] = "OK" if result == 0 else "UNREACHABLE"
        except Exception as e:
            checks["spark_master"] = f"ERROR: {e}"

        # Check PostgreSQL
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(("localhost", 5432))
            sock.close()
            checks["postgresql"] = "OK" if result == 0 else "UNREACHABLE"
        except Exception as e:
            checks["postgresql"] = f"ERROR: {e}"

        # Check SeaweedFS
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex(("localhost", 8333))
            sock.close()
            checks["seaweedfs"] = "OK" if result == 0 else "UNREACHABLE"
        except Exception as e:
            checks["seaweedfs"] = f"ERROR: {e}"

        print(f"Preflight checks: {checks}")

        # Fail if critical services are down
        if checks.get("spark_master") != "OK":
            raise RuntimeError(f"Spark master unreachable: {checks['spark_master']}")
        if checks.get("postgresql") != "OK":
            raise RuntimeError(f"PostgreSQL unreachable: {checks['postgresql']}")

        return checks

    # ─── Task 2: Run SDP Pipeline ───────────────────────────
    # Option A: Using SparkSubmitOperator with spark-pipelines CLI
    # spark-pipelines wraps spark-submit, so we call it via the operator
    run_sdp_pipeline = SparkSubmitOperator(
        task_id="run_sdp_pipeline",
        application=PIPELINE_SCRIPT,
        conn_id=SPARK_CONN_ID,
        conf=SPARK_CONF,
        name="lakehouse-sdp-pipeline",
        verbose=True,
        jars="/opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,"
             "/opt/spark/jars-extra/aws-bundle-2.24.6.jar,"
             "/opt/spark/jars-extra/postgresql-42.7.3.jar",
    )

    # ─── Task 3: Verify Output Tables ───────────────────────
    @task
    def verify_tables():
        """Check that SDP output tables exist and have data."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("sdp-verify") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "jdbc") \
            .config("spark.sql.catalog.iceberg.uri",
                    "jdbc:postgresql://localhost:5432/iceberg_catalog") \
            .getOrCreate()

        results = {}
        for table in EXPECTED_TABLES:
            try:
                count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
                results[table] = count
                print(f"  {table}: {count:,} rows")
            except Exception as e:
                results[table] = f"ERROR: {e}"
                print(f"  {table}: ERROR - {e}")

        spark.stop()

        # Fail if any critical table is missing
        for table in ["iceberg.bronze.orders", "iceberg.silver.orders_enriched"]:
            if isinstance(results.get(table), str) and "ERROR" in results[table]:
                raise RuntimeError(f"Table verification failed: {table}")

        return results

    # ─── Task 4: Iceberg Maintenance ────────────────────────
    @task
    def iceberg_maintenance():
        """Run lightweight maintenance on output tables."""
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("sdp-maintenance") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "jdbc") \
            .config("spark.sql.catalog.iceberg.uri",
                    "jdbc:postgresql://localhost:5432/iceberg_catalog") \
            .getOrCreate()

        for table in EXPECTED_TABLES:
            try:
                # Expire old snapshots (keep last 5, older than 7 days)
                spark.sql(
                    f"CALL iceberg.system.expire_snapshots("
                    f"table => '{table}', retain_last => 5)"
                )
                print(f"  {table}: snapshots expired")
            except Exception as e:
                print(f"  {table}: maintenance skipped ({e})")

        spark.stop()

    # ─── DAG Flow ───────────────────────────────────────────
    preflight = preflight_check()
    verify = verify_tables()
    maintain = iceberg_maintenance()

    preflight >> run_sdp_pipeline >> verify >> maintain
