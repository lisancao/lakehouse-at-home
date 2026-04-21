#!/usr/bin/env python3
"""
OverArchitected Act 5a: Airflow + SDP — Enterprise Orchestration
=================================================================

"How do we schedule this?" Airflow orchestrates, SDP defines pipelines.
This script demonstrates the operator landscape and how to wire SDP into Airflow.

NOTE: This is a reference/demo script that shows Airflow DAG patterns.
The actual DAG file lives at dags/sdp_pipeline.py.

This script can also be run standalone to test the SparkSubmit + SDP
commands that Airflow would execute.

Run (standalone test):
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/overarchitected/05a_airflow_sdp.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse start airflow
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def act5a_operator_landscape():
    """Step 1: The Airflow Spark operator landscape."""
    section("STEP 1: Airflow Spark Operators (Which One?)")

    print("""
  apache-airflow-providers-apache-spark v5.5.1 (March 2026)

  ┌─────────────────────────┬───────────────────────────────────────────┐
  │ Operator                │ Use Case                                  │
  ├─────────────────────────┼───────────────────────────────────────────┤
  │ SparkSubmitOperator     │ Submit any Spark app (scripts, JARs)      │
  │                         │ Wraps spark-submit CLI                    │
  │                         │ Works with all cluster managers            │
  │                         │ THIS IS HOW YOU RUN SDP                   │
  ├─────────────────────────┼───────────────────────────────────────────┤
  │ SparkSqlOperator        │ Run Spark SQL queries directly            │
  │                         │ Wraps spark-sql CLI                       │
  │                         │ Good for DDL, simple transforms           │
  ├─────────────────────────┼───────────────────────────────────────────┤
  │ PysparkOperator (NEW)   │ Run PySpark code inline in the DAG        │
  │ (v5.5.0, Jan 2026)     │ No separate .py file needed               │
  │                         │ Good for small transforms, prototyping     │
  ├─────────────────────────┼───────────────────────────────────────────┤
  │ SparkJDBCOperator       │ Spark ↔ JDBC data transfers               │
  │                         │ Niche — use SparkSubmit for most cases     │
  ├─────────────────────────┼───────────────────────────────────────────┤
  │ SparkKubernetesOperator │ Spark on K8s (separate cncf-k8s provider) │
  │                         │ Direct K8s pod management                  │
  └─────────────────────────┴───────────────────────────────────────────┘

  New DeclarativePipelinesOperator! Just merged in ;).
  SDP runs via SparkSubmitOperator wrapping `spark-pipelines run`.
  This is actually fine — SDP is built on top of spark-submit.
    """)


def act5a_sdp_in_airflow():
    """Step 2: How to wire SDP into an Airflow DAG."""
    section("STEP 2: SDP in Airflow (The DAG)")

    print("""
  Holly asks: "How does Airflow know about my SDP pipeline?"

  It doesn't. Airflow orchestrates WHEN. SDP handles WHAT and HOW.

  ┌──────────────────────────────────────────────────────────────┐
  │  Airflow DAG: lakehouse_sdp_pipeline                         │
  │                                                              │
  │  [preflight_check] → [run_sdp_pipeline] → [verify_tables]   │
  │                                                              │
  │  run_sdp_pipeline = SparkSubmitOperator(                     │
  │      application="spark-pipelines",                          │
  │      application_args=["run",                                │
  │                        "--spec", "/scripts/pipelines/spark-pipeline.yml"],│
  │      conn_id="spark_41",                                     │
  │      conf={                                                  │
  │          "spark.master": "spark://spark-master-41:7078",     │
  │          "spark.sql.catalog.iceberg": "org.apache.iceberg...",│
  │      },                                                      │
  │  )                                                           │
  └──────────────────────────────────────────────────────────────┘

  Separation of concerns:
    - Airflow: schedule, retry, alert, dependency between DAGs
    - SDP: dependency between TABLES, execution order, writes
    - They don't compete — they complement
    """)


def act5a_pyspark_operator():
    """Step 3: The new PysparkOperator (v5.5.0)."""
    section("STEP 3: PysparkOperator (New in v5.5.0)")

    print("""
  Nick asks: "Can I just write PySpark inline in the DAG?"

  Yes. The PysparkOperator (January 2026) lets you do this:

  @task.pyspark(conn_id="spark_41")
  def check_table_freshness(spark: SparkSession):
      latest = spark.sql('''
          SELECT MAX(event_timestamp)
          FROM iceberg.bronze.orders
      ''').collect()[0][0]
      if latest < datetime.now() - timedelta(hours=2):
          raise AirflowException("Data is stale!")
      return str(latest)

  When to use which:
    - PysparkOperator: quick checks, small transforms, prototyping
    - SparkSubmitOperator: production pipelines, SDP, heavy workloads
    - SparkSqlOperator: DDL, simple SQL queries
    """)


def act5a_existing_dags():
    """Step 4: What DAGs we already have."""
    section("STEP 4: Existing Airflow DAGs")

    print("""
  Our lakehouse already has two production DAGs:

  1. lakehouse_medallion_pipeline (dags/lakehouse_medallion_pipeline.py)
     - Schedule: @daily
     - Tasks: check_kafka → choose_spark_version → run_pipeline → verify_tables
     - Supports both Spark 4.0 and 4.1 (branching)

  2. iceberg_maintenance (dags/iceberg_maintenance.py)
     - Schedule: daily at 3 AM
     - Tasks: expire_snapshots → remove_orphan_files → rewrite_data_files
     - Tables: bronze.orders, silver.orders_clean, gold.daily_summary

  3. lakehouse_sdp_pipeline (dags/sdp_pipeline.py) ← NEW
     - Schedule: @daily
     - Tasks: preflight → run_sdp → verify → iceberg_maintenance
     - Uses SparkSubmitOperator to run `spark-pipelines`

  Airflow UI: http://localhost:8085 (admin/admin)
    """)


def act5a_standalone_test(spark):
    """Step 5: Test the SDP command that Airflow would run."""
    section("STEP 5: Testing SDP Command (What Airflow Executes)")

    print("""
  The command Airflow runs under the hood:

    spark-pipelines run \\
        --spec /scripts/pipelines/spark-pipeline.yml \\
        --master spark://spark-master-41:7078

  Or equivalently via spark-submit:

    spark-submit \\
        --master spark://spark-master-41:7078 \\
        --class org.apache.spark.sql.connect.pipelines.PipelinesMain \\
        --conf spark.pipelines.spec=/scripts/pipelines/spark-pipeline.yml \\
        /path/to/spark-pipelines.jar

  Let's verify the pipeline spec exists and is valid:
    """)

    # Check if pipeline spec exists
    try:
        import os
        spec_path = "/scripts/pipelines/spark-pipeline.yml"
        if os.path.exists(spec_path):
            with open(spec_path) as fh:
                print(f"  Pipeline spec ({spec_path}):")
                print(f"  {'─' * 40}")
                for line in fh:
                    print(f"    {line.rstrip()}")
                print(f"  {'─' * 40}")
        else:
            print(f"  Pipeline spec not found at {spec_path}")
    except Exception as e:
        print(f"  Could not read pipeline spec: {e}")

    # Quick sanity check: can we read the bronze tables?
    print("\n  Verifying bronze tables are accessible:")
    for table in ["iceberg.bronze.orders", "iceberg.bronze.dim_locations"]:
        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            print(f"    {table}: {count:,} rows")
        except Exception:
            print(f"    {table}: not found")


def main():
    spark = SparkSession.builder \
        .appName("OverArchitected-05a-AirflowSDP") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("  ACT 5a: ENTERPRISE ORCHESTRATION")
    print("  Airflow schedules. SDP defines. They don't compete.")
    print("=" * 60)
    print(f"  Spark version: {spark.version}")

    act5a_operator_landscape()
    act5a_sdp_in_airflow()
    act5a_pyspark_operator()
    act5a_existing_dags()
    act5a_standalone_test(spark)

    print("\n" + "=" * 60)
    print("  Act 5a complete.")
    print("  Next: Spark Connect + Kubernetes scaling.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
