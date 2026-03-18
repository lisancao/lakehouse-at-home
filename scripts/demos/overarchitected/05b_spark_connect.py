#!/usr/bin/env python3
"""
OverArchitected Act 5b: Spark Connect — Thin Client Progression
================================================================

Demonstrates the Spark Connect progression from laptop to enterprise.
Same DataFrame code at every level — only the session builder changes.

Four levels:
  1. Local: SparkSession.builder.master("local[*]")
  2. Standalone: SparkSession.builder.master("spark://host:7078")
  3. Spark Connect: SparkSession.builder.remote("sc://host:15002")
  4. K8s Connect: SparkSession.builder.remote("sc://k8s-lb:15002")

Run (on the Spark cluster — demonstrates Level 2):
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        /scripts/demos/overarchitected/05b_spark_connect.py

Run (thin client from host — demonstrates Level 3):
    # First, start Spark Connect server:
    docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
        --master spark://spark-master-41:7078 \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0

    # Then from your laptop (1.5 MB install):
    pip install pyspark-client
    python scripts/demos/overarchitected/05b_spark_connect.py --remote sc://localhost:15002

Prerequisites:
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

import sys
from pyspark.sql import SparkSession


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def get_spark_session():
    """Create SparkSession based on CLI args or environment."""
    remote_url = None
    for i, arg in enumerate(sys.argv):
        if arg == "--remote" and i + 1 < len(sys.argv):
            remote_url = sys.argv[i + 1]

    if remote_url:
        print(f"  Mode: Spark Connect (thin client)")
        print(f"  Remote: {remote_url}")
        return SparkSession.builder \
            .remote(remote_url) \
            .getOrCreate()
    else:
        print(f"  Mode: Classic (driver on cluster)")
        return SparkSession.builder \
            .appName("OverArchitected-05b-SparkConnect") \
            .getOrCreate()


def act5b_connect_architecture():
    """Step 1: How Spark Connect works."""
    section("STEP 1: Spark Connect Architecture")

    print("""
  Traditional Spark (Classic Mode):
    ┌─────────────────────────────────────────┐
    │  Your Code + SparkSession + JVM Driver  │ ← Everything in one process
    │  (PySpark uses Py4J to talk to JVM)     │
    │         │                               │
    │    Executors on workers                 │
    └─────────────────────────────────────────┘

  Spark Connect (Client-Server Mode):
    ┌──────────────┐        ┌──────────────────────────┐
    │  Your Laptop │  gRPC  │  SparkConnectServer      │
    │  (Python)    │───────>│  (embedded in driver)    │
    │  No JVM!     │<───────│                          │
    │  1.5 MB      │ Arrow  │  Executors on workers    │
    └──────────────┘        └──────────────────────────┘

  Protocol:
    Client → Server: gRPC + Protocol Buffers (query plan)
    Server → Client: Apache Arrow (result data)

  Port: 15002 (default)
  Connection string: sc://host:15002/;option=value
    """)


def act5b_setup_commands():
    """Step 2: How to set up Spark Connect."""
    section("STEP 2: Setup Commands")

    print("""
  SERVER SIDE (on your Spark cluster):
    # Start the Connect server (already included in Spark images)
    /opt/spark/sbin/start-connect-server.sh \\
        --master spark://spark-master-41:7078 \\
        --jars /opt/spark/jars-extra/iceberg-spark-runtime-4.0_2.13-1.10.0.jar,\\
               /opt/spark/jars-extra/aws-bundle-2.24.6.jar \\
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \\
        --conf spark.sql.catalog.iceberg.type=jdbc \\
        --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://host:5432/iceberg_catalog

    # Stop it
    /opt/spark/sbin/stop-connect-server.sh

  CLIENT SIDE (your laptop):
    pip install pyspark-client    # 1.5 MB, no JVM required!
    # Or: pip install pyspark     # Full install also works

    python -c "
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote('sc://localhost:15002').getOrCreate()
    spark.sql('SELECT COUNT(*) FROM iceberg.bronze.orders').show()
    "

  THAT'S IT. Same API. No JVM on client.
    """)


def act5b_progression_demo(spark):
    """Step 3: Run the same queries regardless of connection mode."""
    section("STEP 3: The Same Code, Any Connection")

    print(f"  Connected via: {'Spark Connect' if hasattr(spark, '_client') else 'Classic mode'}")
    print(f"  Spark version: {spark.version}")

    # The exact same DataFrame code works in all modes
    print("\n  Query 1: Table row counts")
    for table in ["iceberg.bronze.orders", "iceberg.bronze.dim_brands"]:
        try:
            count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {table}").collect()[0][0]
            print(f"    {table}: {count:,} rows")
        except Exception as e:
            print(f"    {table}: {e}")

    print("\n  Query 2: Top 5 brands by order volume")
    try:
        spark.sql("""
            SELECT b.name, COUNT(*) as order_count
            FROM iceberg.bronze.orders o
            JOIN iceberg.bronze.dim_brands b
              ON CAST(
                  get_json_object(o.body, '$.brand_id') AS INT
              ) = b.id
            WHERE o.event_type = 'order_created'
              AND o.body IS NOT NULL
            GROUP BY b.name
            ORDER BY order_count DESC
            LIMIT 5
        """).show(truncate=False)
    except Exception as e:
        print(f"    Query failed: {e}")
        print("    (This is expected if Iceberg tables aren't loaded)")

    print("\n  Query 3: Hourly order distribution")
    try:
        spark.sql("""
            SELECT HOUR(TO_TIMESTAMP(REPLACE(ts, 'T', ' '))) as hour,
                   COUNT(*) as events
            FROM iceberg.bronze.orders
            WHERE event_type = 'order_created'
            GROUP BY 1
            ORDER BY 1
        """).show(24, truncate=False)
    except Exception as e:
        print(f"    Query failed: {e}")

    print("""
  Key point: The queries above are IDENTICAL whether you're running:
    - On the cluster (spark-submit)
    - From your laptop via Spark Connect
    - From a K8s pod via Spark Connect

  Your code doesn't change. The infrastructure does.
    """)


def act5b_connect_vs_classic():
    """Step 4: What you gain and lose with Spark Connect."""
    section("STEP 4: Connect vs Classic — Tradeoffs")

    print("""
  WHAT YOU GAIN:
    + No JVM on client (1.5 MB install vs 300+ MB)
    + Language-native experience (pure Python, no Py4J)
    + Client crashes don't kill the Spark application
    + Multiple clients can share one Spark session/cluster
    + Clean separation: laptop ↔ cluster

  WHAT YOU LOSE:
    - No RDD API (DataFrame/SQL only — you shouldn't be using RDDs anyway)
    - No SparkContext access
    - No direct JVM access (no df._jdf, no custom Catalyst rules)
    - Security is bring-your-own (gRPC proxy for auth)
    - Some edge-case APIs may not be available

  WHAT STAYS THE SAME:
    = DataFrame operations
    = SQL queries
    = Python UDFs
    = Structured Streaming
    = Catalog operations (SHOW TABLES, CREATE TABLE, etc.)
    = ML (pyspark.ml — new in Spark 4.0)

  For 95% of data engineering work: Spark Connect is strictly better.
    """)


def act5b_k8s_preview():
    """Step 5: Preview of K8s deployment."""
    section("STEP 5: K8s Deployment (Preview)")

    print("""
  The full progression (same code at every level):

  ┌─────────────┬──────────────────────────────────────────────┐
  │ Level       │ Session Builder                               │
  ├─────────────┼──────────────────────────────────────────────┤
  │ Laptop      │ .master("local[*]")                          │
  │ Standalone  │ .master("spark://host:7078")                 │
  │ Connect     │ .remote("sc://host:15002")                   │
  │ K8s         │ .remote("sc://k8s-loadbalancer:15002")       │
  └─────────────┴──────────────────────────────────────────────┘

  K8s deployment:
    1. Deploy SparkConnectServer as a K8s Deployment
    2. Expose port 15002 via LoadBalancer Service
    3. spark-submit --master k8s://https://api-server:6443 (server-side)
    4. pip install pyspark-client (client-side)
    5. SparkSession.builder.remote("sc://k8s-lb:15002")

  For SDP on K8s:
    spark-pipelines run --spec pipeline.yml \\
        --master k8s://https://api-server:6443 \\
        --conf spark.kubernetes.container.image=apache/spark:4.1.0 \\
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark

  Same pipeline. Same spec. Different master URL.
  See 05c_spark_k8s.sh for full setup commands.
    """)


def main():
    spark = get_spark_session()
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass  # Connect mode may not support setLogLevel

    print("\n" + "=" * 60)
    print("  ACT 5b: SPARK CONNECT")
    print("  Same code. Any connection. No JVM on client.")
    print("=" * 60)

    act5b_connect_architecture()
    act5b_setup_commands()
    act5b_progression_demo(spark)
    act5b_connect_vs_classic()
    act5b_k8s_preview()

    print("\n" + "=" * 60)
    print("  Act 5b complete.")
    print("  Next: we're lazy — let AI manage this.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
