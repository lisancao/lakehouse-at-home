#!/usr/bin/env python3
"""
OverArchitected Act 2: "We Need a Catalog" — Unity Catalog OSS
===============================================================

Holly and Nick have data but no governance. They need a catalog.
Unity Catalog OSS provides: REST catalog, credential vending, multi-engine interop.

Demonstrates:
  1. UC REST API — list catalogs, schemas, tables
  2. Register Iceberg tables via REST catalog
  3. Credential vending — UC gates access to object storage
  4. Multi-engine interop — same table from Spark AND DuckDB-compatible clients
  5. Catalog-managed commits (experimental)
  6. Honest gaps — what OSS UC doesn't have (yet)

Run:
    # Start UC first:
    ./lakehouse start unity-catalog

    # Then run this script against Spark configured for UC:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        --conf spark.sql.catalog.unity=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.unity.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
        --conf spark.sql.catalog.unity.uri=http://localhost:8081/api/2.1/unity-catalog/iceberg \
        /scripts/demos/overarchitected/02_unity_catalog_setup.py

    # Or use the UC spark config:
    docker exec spark-master-41 /opt/spark/bin/spark-submit \
        --properties-file /opt/spark/conf/spark-defaults-uc.conf \
        /scripts/demos/overarchitected/02_unity_catalog_setup.py

Prerequisites:
    ./lakehouse start all
    ./lakehouse start unity-catalog
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

from pyspark.sql import SparkSession
import json

# UC REST API base URL (default port)
UC_BASE_URL = "http://localhost:8081"
UC_API = f"{UC_BASE_URL}/api/2.1/unity-catalog"
UC_ICEBERG_API = f"{UC_BASE_URL}/api/2.1/unity-catalog/iceberg"


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def uc_rest_call(endpoint, method="GET", data=None):
    """Make a REST call to Unity Catalog API. Returns dict or None on error."""
    import urllib.request
    import urllib.error

    url = f"{UC_API}/{endpoint}"
    try:
        req = urllib.request.Request(url, method=method)
        req.add_header("Content-Type", "application/json")
        if data:
            req.data = json.dumps(data).encode("utf-8")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.URLError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


def act2_check_connectivity():
    """Step 0: Is UC running?"""
    section("STEP 0: Check Unity Catalog Connectivity")

    result = uc_rest_call("catalogs")
    if "error" in result:
        print(f"  UC not reachable at {UC_BASE_URL}")
        print(f"  Error: {result['error']}")
        print(f"  Run: ./lakehouse start unity-catalog")
        return False

    catalogs = result.get("catalogs", [])
    print(f"  UC is running at {UC_BASE_URL}")
    print(f"  Catalogs found: {len(catalogs)}")
    for cat in catalogs:
        print(f"    - {cat.get('name', 'unknown')}")
    return True


def act2_setup_steps():
    """Step 1: How many steps to set up UC? Show the answer."""
    section("STEP 1: Unity Catalog Setup (How Hard Is It?)")

    print("""
  Nick asks: "How hard is this to set up?"

  Three commands:

    1. Start the server:
       docker compose -f docker-compose-unity-catalog.yml up -d

    2. Verify it's running:
       curl http://localhost:8081/api/2.1/unity-catalog/catalogs

    3. Point Spark at it:
       spark.sql.catalog.iceberg.catalog-impl = org.apache.iceberg.rest.RESTCatalog
       spark.sql.catalog.iceberg.uri = http://localhost:8081/api/2.1/unity-catalog/iceberg

  That's it. A default catalog called 'unity' ships out of the box.
  No database migrations. No config files. No cloud account.
    """)


def act2_explore_catalog(spark):
    """Step 2: Explore what's in the catalog via REST + Spark SQL."""
    section("STEP 2: Explore the Catalog")

    # REST API exploration
    print("  Via REST API:")
    catalogs = uc_rest_call("catalogs")
    if "catalogs" in catalogs:
        for cat in catalogs["catalogs"]:
            cat_name = cat.get("name", "unknown")
            print(f"\n  Catalog: {cat_name}")

            schemas = uc_rest_call(f"schemas?catalog_name={cat_name}")
            if "schemas" in schemas:
                for schema in schemas["schemas"]:
                    schema_name = schema.get("name", "unknown")
                    print(f"    Schema: {schema_name}")

                    tables = uc_rest_call(
                        f"tables?catalog_name={cat_name}&schema_name={schema_name}"
                    )
                    if "tables" in tables:
                        for tbl in tables["tables"]:
                            tbl_name = tbl.get("name", "unknown")
                            tbl_type = tbl.get("table_type", "unknown")
                            data_format = tbl.get("data_source_format", "unknown")
                            print(f"      Table: {tbl_name} ({tbl_type}, {data_format})")

    # Spark SQL exploration (if UC catalog is configured)
    print("\n  Via Spark SQL:")
    try:
        spark.sql("SHOW NAMESPACES IN unity").show(truncate=False)
    except Exception as e:
        print(f"    Spark not configured for UC catalog 'unity': {e}")
        print("    This is expected if running with default JDBC catalog config.")
        print("    To use UC: pass --properties-file /opt/spark/conf/spark-defaults-uc.conf")


def act2_register_tables(spark):
    """Step 3: Register tables in UC."""
    section("STEP 3: Register Tables in Unity Catalog")

    print("""
  Two ways to register tables in UC:

  A) REST API — programmatic, any language:
     POST /api/2.1/unity-catalog/tables
     {"name": "orders", "catalog_name": "unity", "schema_name": "bronze",
      "table_type": "EXTERNAL", "data_source_format": "ICEBERG",
      "storage_location": "s3://lakehouse/warehouse/bronze/orders"}

  B) Spark SQL — if Spark is configured with UC as catalog:
     CREATE TABLE unity.bronze.orders USING iceberg
     LOCATION 's3://lakehouse/warehouse/bronze/orders'
    """)

    # Try registering via REST
    print("  Registering schemas via REST API...")

    # Create bronze schema if not exists
    for schema_name in ["bronze", "silver", "gold"]:
        result = uc_rest_call("schemas", method="POST", data={
            "name": schema_name,
            "catalog_name": "unity",
            "comment": f"Medallion {schema_name} layer — Casper's Kitchen data",
        })
        if "error" in result:
            # Schema may already exist
            print(f"    {schema_name}: already exists or error")
        else:
            print(f"    {schema_name}: created")


def act2_credential_vending():
    """Step 4: Credential vending — UC gates storage access."""
    section("STEP 4: Credential Vending")

    print("""
  Nick asks: "How do we control who accesses the raw files?"

  Unity Catalog credential vending (overhauled in v0.4.0):

  NEW in 0.4.0 — two-layer credential model:
    1. Storage Credentials: define HOW to access storage (IAM role, keys)
       POST /api/2.1/unity-catalog/credentials
       {"name": "seaweedfs-creds", "aws_iam_role": {...}}

    2. External Locations: define WHERE storage is + bind to credentials
       POST /api/2.1/unity-catalog/external-locations
       {"name": "lakehouse-data", "url": "s3://lakehouse/warehouse",
        "credential_name": "seaweedfs-creds"}

  The flow:
    - Client requests access to a table
    - UC validates via external location matching
    - UC returns short-lived, scoped storage credentials
    - Client uses those credentials to read/write data
    - Credentials expire — no permanent keys floating around

  How it works with Iceberg REST Catalog:
    1. Client calls GET /v1/namespaces/bronze/tables/orders
    2. Response includes temporary S3 credentials
    3. Client reads parquet files using those credentials
    4. No direct S3 access needed — UC is the gatekeeper

  In our setup (SeaweedFS):
    - UC vends S3-compatible credentials for SeaweedFS
    - Same protocol as AWS S3, Azure ADLS, or GCS
    - Credential rotation is automatic
    - AWS IAM role support added in 0.4.0
    """)

    # Show the Iceberg REST catalog endpoint
    print("  Iceberg REST Catalog endpoint:")
    print(f"    {UC_ICEBERG_API}")
    print("    This is what Spark, DuckDB, Trino, etc. all connect to.")
    print("    One endpoint, many engines, credential vending built in.")


def act2_multi_engine():
    """Step 5: Multi-engine interop — the killer feature."""
    section("STEP 5: Multi-Engine Interop (The Killer Feature)")

    print("""
  Holly asks: "Can DuckDB read our tables too?"

  YES. That's the whole point of the Iceberg REST Catalog protocol.

  From Spark:
    spark.sql("SELECT * FROM unity.bronze.orders LIMIT 5")

  From DuckDB:
    INSTALL iceberg;
    LOAD iceberg;
    ATTACH 'http://localhost:8081/api/2.1/unity-catalog/iceberg'
      AS unity (TYPE ICEBERG);
    SELECT * FROM unity.bronze.orders LIMIT 5;

  From Trino:
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://localhost:8081/api/2.1/unity-catalog/iceberg

  From Polars:
    import polars as pl
    df = pl.scan_iceberg(
        "unity.bronze.orders",
        storage_options={"uri": "http://localhost:8081/..."}
    )

  Same table. Same data. Same catalog. Different engines.
  This is what open standards buy you.
    """)


def act2_catalog_managed_commits():
    """Step 6: Catalog-managed commits (experimental)."""
    section("STEP 6: Catalog-Managed Commits (Experimental)")

    print("""
  Nick asks: "What if Spark and DuckDB write at the same time?"

  Catalog-managed commits (flagship feature in UC 0.4.0):
    - UC server centrally coordinates table commits
    - Multiple engines can safely write to the same table
    - No commit conflicts — UC is the single authority
    - Enabled via: server.managed-table.enabled=true

  The protocol (Delta tables):
    1. Client calls POST /staging-tables (UC assigns storage path)
    2. Client writes data + delta log
    3. Client calls POST /tables to finalize
    4. Subsequent writes: staged commits → POST /delta/preview/commits to ratify
    5. Reads combine published + ratified commits

  For Iceberg tables via REST Catalog:
    - The REST catalog protocol inherently coordinates commits
    - The catalog server IS the commit authority
    - This is baked into the Iceberg REST spec — not a UC-specific hack

  UniForm (NEW in 0.4.0):
    - Delta commits can atomically update Iceberg metadata
    - Write as Delta, read as Iceberg — transparent to downstream engines

  Status: Production-ready for Delta. Iceberg REST reads solid.
  Multi-writer scenarios: test thoroughly.
    """)


def act2_honest_gaps():
    """Step 7: What UC OSS does NOT have."""
    section("STEP 7: The Honest Gaps")

    print("""
  Holly asks: "So this is just like Databricks Unity Catalog?"

  No. Here's what OSS UC 0.4.0 gives you:
    [x] Iceberg REST Catalog (strong, production-ready)
    [x] Credential vending via external locations (overhauled in 0.4.0)
    [x] Storage credentials API with IAM role support
    [x] Multi-engine interop (Spark, DuckDB, Trino, etc.)
    [x] Catalog-managed commits (Delta — flagship 0.4.0 feature)
    [x] Managed storage locations for catalogs/schemas
    [x] UniForm (Delta-to-Iceberg atomic metadata)
    [x] Authorization framework (OAuth + grants)
    [x] Schema/table/volume management
    [x] Model registry (MLflow integration)
    [x] Helm charts for K8s deployment

  Here's what it does NOT have (roadmapped for v0.5+):
    [ ] Full RBAC with row-level filters / column masks
    [ ] Audit logging
    [ ] Data lineage
    [ ] Delta Sharing
    [ ] Lakehouse Federation
    [ ] Group management / service principals
    [ ] Native Iceberg writes (only Delta-via-UniForm)
    [ ] Multi-tenancy

  The honest take:
    UC OSS 0.4.0 is a real catalog with credential vending,
    managed commits, and multi-engine interop. It's come a
    long way from 0.1. But full governance (RBAC, audit,
    lineage) is still in the future.

  That's not a failure — that's the reality of open source vs managed.
  You get a solid foundation. The rest is yours to build or buy.
    """)


def main():
    spark = SparkSession.builder \
        .appName("OverArchitected-02-UnityCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 60)
    print("  ACT 2: WE NEED A CATALOG")
    print("  Unity Catalog OSS — open, extensible, honest.")
    print("=" * 60)
    print(f"  Spark version: {spark.version}")

    # Check connectivity first
    uc_available = act2_check_connectivity()

    # Always show the setup steps (works without UC running)
    act2_setup_steps()

    if uc_available:
        act2_explore_catalog(spark)
        act2_register_tables(spark)

    # These sections are informational — work without UC running
    act2_credential_vending()
    act2_multi_engine()
    act2_catalog_managed_commits()
    act2_honest_gaps()

    print("\n" + "=" * 60)
    print("  Act 2 complete.")
    print("  Next: we need COMPUTE to process this data.")
    print("=" * 60 + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
