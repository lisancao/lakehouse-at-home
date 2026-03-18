# Companion Guide: Unity Catalog OSS

**OverArchitected Show -- Act 2: "We Need a Catalog"**

| | |
|---|---|
| **Audience** | Databricks users who know managed Unity Catalog but have never set up the open-source version. |
| **Complements** | Demo script `scripts/demos/overarchitected/02_unity_catalog_setup.py` |
| **UC version covered** | 0.3.1 (February 2026) |
| **Last updated** | 2026-03-18 |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Architecture](#2-architecture)
3. [Setup Guide](#3-setup-guide)
4. [Iceberg REST Catalog](#4-iceberg-rest-catalog)
5. [Credential Vending](#5-credential-vending)
6. [Catalog-Managed Commits](#6-catalog-managed-commits)
7. [Multi-Engine Interop](#7-multi-engine-interop)
8. [Governance Capabilities](#8-governance-capabilities)
9. [Model Registry and MLflow Integration](#9-model-registry-and-mlflow-integration)
10. [The Honest Gaps vs Databricks UC](#10-the-honest-gaps-vs-databricks-uc)
11. [Version History and Roadmap](#11-version-history-and-roadmap)
12. [Integration with lakehouse-stack](#12-integration-with-lakehouse-stack)
13. [CLI Reference](#13-cli-reference)
14. [Troubleshooting](#14-troubleshooting)
15. [References](#15-references)

---

## 1. Introduction

### What Is Unity Catalog OSS?

Unity Catalog OSS is an open-source, vendor-neutral catalog for data and AI assets. It provides a REST-based metadata layer that multiple engines (Spark, DuckDB, Trino, Polars, Dremio, and others) can share to discover tables, manage schemas, and obtain storage credentials -- all without vendor lock-in.

The project implements the three-level namespace model (`catalog.schema.table`) familiar to every Databricks user, but runs as a standalone Java server that you deploy yourself.

**Repository:** [github.com/unitycatalog/unitycatalog](https://github.com/unitycatalog/unitycatalog)
**Documentation:** [docs.unitycatalog.io](https://docs.unitycatalog.io/)
**License:** Apache 2.0

### History

| Date | Event | Source |
|------|-------|--------|
| 2024-06-12 | Databricks open-sources Unity Catalog at the Data + AI Summit. Initial release: v0.1. | [Databricks blog: "Open Sourcing Unity Catalog"](https://www.databricks.com/blog/open-sourcing-unity-catalog) |
| 2024-08-15 | Unity Catalog accepted into the Linux Foundation's AI & Data organization as an incubating project. | [LF AI & Data announcement](https://lfaidata.foundation/blog/2024/08/15/unity-catalog-joins-lf-ai-data-foundation/) |
| 2024-10-10 | v0.2 released. Adds credential vending for S3/Azure/GCS, MLflow model registry support, and the initial DuckDB integration. | [UC 0.2 release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.2.0) |
| 2025-05-22 | v0.3 released. Experimental catalog-managed commits, improved Iceberg REST spec compliance, PostgreSQL backend support. | [UC 0.3 release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.3.0) |
| 2025-09-18 | v0.3.1 released. Bug fixes, improved credential vending reliability, Iceberg 1.7+ compatibility fixes. | [UC 0.3.1 release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.3.1) |
| 2026-02-06 | v0.3.1 maintenance update with Iceberg 1.10 compatibility. This is the version used in lakehouse-stack. | [Docker Hub: unitycatalog/unitycatalog:0.3.1](https://hub.docker.com/r/unitycatalog/unitycatalog) |

### Why Does This Exist?

Databricks Unity Catalog (managed) is tightly integrated with the Databricks runtime. You cannot run it outside Databricks. You cannot point DuckDB at it without going through Databricks-hosted endpoints. You cannot self-host it.

UC OSS exists to solve the catalog interoperability problem: give every engine a single place to find tables, get credentials, and coordinate writes -- without requiring any specific vendor's runtime.

The practical value proposition for self-hosted lakehouses:

- **One catalog, many engines.** Register a table once; read it from Spark, DuckDB, Trino, Polars, or any engine that speaks the Iceberg REST protocol.
- **Credential vending.** Stop distributing long-lived S3 keys to every user and application. UC issues short-lived, scoped credentials on demand.
- **Open standard.** The Iceberg REST Catalog API is an Apache-governed specification. UC OSS is one implementation; you are not locked to it.

---

## 2. Architecture

### High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Engines                               │
│   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌────────┐ │
│   │  Spark  │  │ DuckDB  │  │  Trino  │  │ Polars  │  │ Dremio │ │
│   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └───┬────┘ │
│        │             │            │             │            │       │
└────────┼─────────────┼────────────┼─────────────┼────────────┼──────┘
         │             │            │             │            │
         └─────────────┴─────┬──────┴─────────────┴────────────┘
                             │
                    HTTP REST API (port 8080)
                             │
┌────────────────────────────┼────────────────────────────────────────┐
│                    Unity Catalog Server                              │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   REST API Layer                               │  │
│  │                                                               │  │
│  │  ┌─────────────────────┐    ┌──────────────────────────────┐ │  │
│  │  │  UC Native API      │    │  Iceberg REST Catalog API    │ │  │
│  │  │                     │    │                              │ │  │
│  │  │  /api/2.1/unity-    │    │  /api/2.1/unity-catalog/     │ │  │
│  │  │  catalog/           │    │  iceberg/v1/                 │ │  │
│  │  │                     │    │                              │ │  │
│  │  │  - /catalogs        │    │  - /namespaces               │ │  │
│  │  │  - /schemas         │    │  - /tables                   │ │  │
│  │  │  - /tables          │    │  - /views                    │ │  │
│  │  │  - /volumes         │    │  - /config                   │ │  │
│  │  │  - /functions       │    │  - /transactions (0.3+)      │ │  │
│  │  │  - /models          │    │                              │ │  │
│  │  └─────────────────────┘    └──────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                 Service Layer                                  │  │
│  │                                                               │  │
│  │  ┌──────────────┐  ┌─────────────────┐  ┌─────────────────┐ │  │
│  │  │  Credential  │  │  Table Metadata │  │  Authorization  │ │  │
│  │  │  Vending     │  │  Management     │  │  (basic)        │ │  │
│  │  │  Service     │  │                 │  │                 │ │  │
│  │  │              │  │  - Iceberg      │  │  - Token auth   │ │  │
│  │  │  - S3 STS    │  │  - Delta Lake   │  │  - No RBAC yet  │ │  │
│  │  │  - Azure SAS │  │  - Hudi (read)  │  │                 │ │  │
│  │  │  - GCS OAuth │  │                 │  │                 │ │  │
│  │  └──────────────┘  └─────────────────┘  └─────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                 Persistence Layer                              │  │
│  │                                                               │  │
│  │  ┌──────────────────────────────────────────────────────────┐ │  │
│  │  │  Metadata Store (Hibernate)                              │ │  │
│  │  │                                                          │ │  │
│  │  │  Default: H2 (embedded, file-based)                      │ │  │
│  │  │  Production: PostgreSQL, MySQL (since 0.3)               │ │  │
│  │  └──────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                      │
└─────────────────────────────────┬────────────────────────────────────┘
                                  │
                          Credential Vending
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Object Storage                                  │
│                                                                      │
│   ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────┐  │
│   │  AWS S3   │  │  Azure    │  │  GCS      │  │  SeaweedFS    │  │
│   │           │  │  ADLS     │  │           │  │  (S3-compat)  │  │
│   └───────────┘  └───────────┘  └───────────┘  └───────────────┘  │
│                                                                      │
│   Data files: Parquet, ORC, Avro                                     │
│   Metadata files: Iceberg manifests, manifest lists, table metadata  │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Server Components

The UC server is a single Java process (JVM) with these internal components:

| Component | Responsibility | Implementation |
|-----------|---------------|----------------|
| **REST API Router** | Routes HTTP requests to the appropriate handler. Serves both the UC native API and the Iceberg REST Catalog API on a single port. | Armeria HTTP server ([Armeria docs](https://armeria.dev/)) |
| **UC Native API** | CRUD operations on catalogs, schemas, tables, volumes, functions, and registered models. This is the management API. | Custom handlers at `/api/2.1/unity-catalog/` |
| **Iceberg REST Catalog API** | Implements the Apache Iceberg REST Catalog specification. This is what query engines connect to for Iceberg table access. | Handlers at `/api/2.1/unity-catalog/iceberg/v1/` conforming to [Iceberg REST Open API spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) |
| **Credential Vending Service** | Generates short-lived, scoped credentials for object storage access. Supports S3 (STS AssumeRole or static key scoping), Azure (SAS tokens), and GCS (OAuth2 downscoped tokens). | Internal service invoked during table load responses |
| **Metadata Store** | Persists catalog, schema, table, and function metadata. Uses Hibernate ORM. Default backend is embedded H2; PostgreSQL and MySQL are supported for production. | Hibernate + configurable JDBC backend |
| **Authorization** | Token-based authentication. Basic access control lists. No RBAC in current version. | Token validation middleware |

### Storage Model

UC follows the Databricks three-level namespace model:

```
catalog
  └── schema (database)
       ├── table
       │     ├── metadata_location (pointer to Iceberg/Delta metadata)
       │     ├── storage_location  (S3/ADLS/GCS path to data files)
       │     ├── table_type        (MANAGED | EXTERNAL)
       │     └── data_source_format (ICEBERG | DELTA | HUDI | CSV | JSON | ...)
       ├── volume
       │     ├── storage_location
       │     └── volume_type       (MANAGED | EXTERNAL)
       ├── function
       │     ├── input_params
       │     ├── data_type (return type)
       │     └── routine_body      (SQL | EXTERNAL)
       └── registered_model
             ├── model_versions[]
             └── storage_location  (MLflow artifact path)
```

**Key distinction from managed UC:** In OSS, there is no workspace concept. A single UC server manages one or more catalogs directly. There is no multi-workspace federation.

### REST API Design

UC exposes two API surfaces on a single HTTP port:

**1. UC Native API** (`/api/2.1/unity-catalog/`)

This API manages the catalog metadata itself: creating catalogs, schemas, tables, volumes, functions, and registered models. It follows the Databricks Unity Catalog REST API specification, meaning existing tools built for Databricks UC can potentially target UC OSS with a URL change.

Reference: [UC API specification](https://docs.unitycatalog.io/api/)

**2. Iceberg REST Catalog API** (`/api/2.1/unity-catalog/iceberg/v1/`)

This implements the Apache Iceberg REST Catalog specification ([OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)). Any engine that speaks the Iceberg REST protocol can use UC as its catalog without any UC-specific client code.

This is the API you configure query engines to use. Spark's `RESTCatalog`, DuckDB's `iceberg` extension, Trino's `iceberg` connector, and Polars all speak this protocol natively.

**API versioning:** The `/api/2.1/` prefix matches the Databricks REST API version 2.1. This is not a UC OSS version number -- it is the API contract version.

---

## 3. Setup Guide

### Option A: Docker Compose (Recommended)

This is the fastest path and the method used in lakehouse-stack.

**Step 1: Pull and start the container.**

```bash
# Using lakehouse-stack CLI:
./lakehouse start unity-catalog

# Or directly with Docker Compose:
docker compose -f docker-compose-unity-catalog.yml up -d
```

The `docker-compose-unity-catalog.yml` file:

```yaml
services:
  unity-catalog:
    image: unitycatalog/unitycatalog:0.3.1
    container_name: unity-catalog
    ports:
      - "8080:8080"
    environment:
      - JAVA_OPTS=-Xmx2g
    volumes:
      - ./config/unity-catalog:/opt/unitycatalog/etc/conf
      - uc-data:/opt/unitycatalog/etc/db
      - uc-logs:/opt/unitycatalog/etc/logs
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/api/2.1/unity-catalog/catalogs"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 30s
    restart: unless-stopped
    networks:
      - lakehouse-network
```

**Step 2: Configure `server.properties`.**

```bash
cp config/unity-catalog/server.properties.example config/unity-catalog/server.properties
```

Edit `config/unity-catalog/server.properties`:

```properties
# Server settings
server.env=dev
server.port=8080

# S3-compatible storage (SeaweedFS in lakehouse-stack)
s3.bucketPath.0=s3://lakehouse/warehouse
s3.region.0=us-east-1
s3.accessKey.0=your_seaweedfs_access_key
s3.secretKey.0=your_seaweedfs_secret_key
s3.endpoint.0=http://localhost:8333

# For production: use PostgreSQL instead of embedded H2
# hibernate.connection.driver_class=org.postgresql.Driver
# hibernate.connection.url=jdbc:postgresql://localhost:5432/unity_catalog
# hibernate.connection.username=postgres
# hibernate.connection.password=your_password
# hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
```

**Step 3: Verify.**

```bash
# Health check
curl http://localhost:8080/api/2.1/unity-catalog/catalogs

# Expected response:
# {"catalogs":[{"name":"unity","comment":"Main catalog",...}]}
```

**Step 4: Configure Spark to use UC.**

```bash
cp config/spark/spark-defaults-uc.conf.example config/spark/spark-defaults.conf
```

Or add these properties to your existing Spark config:

```properties
spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.iceberg.uri             http://localhost:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.iceberg.warehouse       unity
spark.sql.catalog.iceberg.token           not_used
```

### Option B: Binary Install (No Docker)

For environments where Docker is not available or not desired.

**Step 1: Install Java 17.**

```bash
# Ubuntu/Debian
sudo apt install openjdk-17-jdk

# macOS
brew install openjdk@17

# Verify
java -version
# openjdk version "17.0.x" ...
```

**Step 2: Download or build UC.**

```bash
# Clone the repository
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog
git checkout v0.3.1

# Build
build/sbt package

# Or download a pre-built release
wget https://github.com/unitycatalog/unitycatalog/releases/download/v0.3.1/unitycatalog-0.3.1.tar.gz
tar -xzf unitycatalog-0.3.1.tar.gz
cd unitycatalog-0.3.1
```

**Step 3: Configure `etc/conf/server.properties`.**

Same format as the Docker example above.

**Step 4: Start the server.**

```bash
bin/start-uc-server
```

The server logs to stdout by default. Use `nohup` or a systemd unit for production.

**Step 5: Verify.**

```bash
curl http://localhost:8080/api/2.1/unity-catalog/catalogs
```

### Option C: Kubernetes (Helm)

The UC project provides a community Helm chart:

```bash
helm repo add unitycatalog https://unitycatalog.github.io/unitycatalog
helm install uc unitycatalog/unitycatalog \
  --set server.port=8080 \
  --set storage.s3.bucketPath=s3://your-bucket/warehouse \
  --set storage.s3.region=us-east-1
```

Reference: [UC Helm chart](https://github.com/unitycatalog/unitycatalog/tree/main/helm)

### Configuration Reference: `server.properties`

| Property | Description | Default | Required |
|----------|-------------|---------|----------|
| `server.env` | Environment name (`dev`, `staging`, `prod`). Affects default logging level. | `dev` | No |
| `server.port` | HTTP listen port. | `8080` | No |
| `s3.bucketPath.N` | S3 bucket and prefix for storage location N. UC maps table locations to a configured bucket by prefix matching. | (none) | Yes (at least one) |
| `s3.region.N` | AWS region for bucket N. | `us-east-1` | Yes |
| `s3.accessKey.N` | AWS access key (or S3-compatible key) for bucket N. | (none) | Yes |
| `s3.secretKey.N` | AWS secret key for bucket N. | (none) | Yes |
| `s3.endpoint.N` | Custom S3 endpoint URL. Required for S3-compatible stores like SeaweedFS or MinIO. Omit for real AWS S3. | (AWS default) | Conditional |
| `s3.sessionToken.N` | AWS session token for temporary credentials. | (none) | No |
| `azure.storageAccount.N` | Azure storage account name. | (none) | Conditional |
| `azure.tenantId.N` | Azure tenant ID. | (none) | Conditional |
| `azure.clientId.N` | Azure client (application) ID. | (none) | Conditional |
| `azure.clientSecret.N` | Azure client secret. | (none) | Conditional |
| `gcs.jsonKeyFilePath.N` | Path to GCS service account JSON key file. | (none) | Conditional |
| `hibernate.connection.driver_class` | JDBC driver class for the metadata store. | H2 driver | No |
| `hibernate.connection.url` | JDBC URL for the metadata store. | H2 file-based URL | No |
| `hibernate.connection.username` | Database username. | `sa` | No |
| `hibernate.connection.password` | Database password. | (empty) | No |
| `hibernate.dialect` | Hibernate SQL dialect. | H2 dialect | No |
| `authorization.enabled` | Enable token-based authorization. | `false` | No |

**Multiple storage locations:** You can configure multiple buckets by incrementing the index suffix (`.0`, `.1`, `.2`, etc.). UC matches table storage locations to the correct bucket configuration by prefix.

---

## 4. Iceberg REST Catalog

### What It Is

The Iceberg REST Catalog is an HTTP-based protocol defined by the Apache Iceberg project. It specifies how a client discovers namespaces, loads table metadata, and commits updates -- all over HTTP.

**Specification:** [Apache Iceberg REST Catalog Open API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

This is the single most important feature of UC OSS. It is what makes multi-engine interop possible without engine-specific plugins.

### How It Works

The protocol flow for reading a table:

```
Client (Spark/DuckDB/Trino)              Unity Catalog Server
         │                                        │
         │  GET /v1/config                        │
         │──────────────────────────────────────>  │
         │  {"defaults": {"warehouse": "unity"}}   │
         │  <──────────────────────────────────────│
         │                                        │
         │  GET /v1/namespaces/bronze/tables/orders│
         │──────────────────────────────────────>  │
         │                                        │
         │  Response:                              │
         │  {                                      │
         │    "metadata-location": "s3://...",      │
         │    "metadata": { ... table schema ... }, │
         │    "config": {                          │
         │      "s3.access-key-id": "ASIA...",     │  ← credential vending
         │      "s3.secret-access-key": "...",     │
         │      "s3.session-token": "...",         │  ← short-lived token
         │      "s3.endpoint": "http://..."        │
         │    }                                    │
         │  }                                      │
         │  <──────────────────────────────────────│
         │                                        │
         │  (Client reads Parquet files from S3    │
         │   using vended credentials)             │
         │                                        │
```

Key insight: the table load response includes both the metadata (schema, partition spec, sort order, snapshots) and the storage credentials needed to read the data files. The client never needs pre-configured S3 keys.

### Spec Compliance in UC 0.3.1

UC 0.3.1 implements the following Iceberg REST Catalog endpoints:

| Endpoint | Method | Status | Notes |
|----------|--------|--------|-------|
| `/v1/config` | GET | Supported | Returns catalog configuration |
| `/v1/namespaces` | GET | Supported | List namespaces |
| `/v1/namespaces` | POST | Supported | Create namespace |
| `/v1/namespaces/{ns}` | GET | Supported | Get namespace metadata |
| `/v1/namespaces/{ns}` | DELETE | Supported | Drop namespace |
| `/v1/namespaces/{ns}/properties` | POST | Supported | Update namespace properties |
| `/v1/namespaces/{ns}/tables` | GET | Supported | List tables in namespace |
| `/v1/namespaces/{ns}/tables` | POST | Supported | Create table |
| `/v1/namespaces/{ns}/tables/{table}` | GET | Supported | Load table (with credential vending) |
| `/v1/namespaces/{ns}/tables/{table}` | DELETE | Supported | Drop table |
| `/v1/namespaces/{ns}/tables/{table}` | POST | Supported | Update table / commit |
| `/v1/tables/rename` | POST | Supported | Rename table |
| `/v1/namespaces/{ns}/tables/{table}/metrics` | POST | Supported | Report scan metrics |
| `/v1/transactions/commit` | POST | Experimental | Catalog-managed commits (0.3+) |
| `/v1/namespaces/{ns}/views` | GET | Partial | List views (basic support) |
| `/v1/namespaces/{ns}/views` | POST | Partial | Create view (basic support) |

**What is NOT implemented:**

- Multi-table transactions (atomic cross-table commits)
- Server-side table scan planning (`/v1/namespaces/{ns}/tables/{table}/plan`)
- Full view management (views are metadata-only, no materialization)

### Endpoint URL

In lakehouse-stack, the Iceberg REST Catalog endpoint is:

```
http://localhost:8080/api/2.1/unity-catalog/iceberg
```

Note the path structure: `/api/2.1/unity-catalog/iceberg` is the base URL. The Iceberg REST spec paths (`/v1/namespaces`, `/v1/tables`, etc.) are appended to this base. Most client libraries handle this automatically when you provide the base URI.

### Namespace Mapping

The three-level UC namespace maps to the Iceberg REST Catalog as follows:

| UC Concept | Iceberg REST Concept | Example |
|------------|---------------------|---------|
| Catalog | Warehouse (configured at connection time) | `unity` |
| Schema | Namespace | `bronze` |
| Table | Table | `orders` |

When you configure a Spark catalog with `warehouse=unity`, the catalog name is resolved server-side. Subsequent calls use the two-level `namespace.table` path:

```
GET /v1/namespaces/bronze/tables/orders
```

For nested namespaces (e.g., `bronze.raw`), the namespace segments are separated by the ASCII Unit Separator character (`%1F`) in the URL path, per the Iceberg REST spec.

---

## 5. Credential Vending

### What It Solves

Without credential vending, every application that reads from the lakehouse needs direct access to object storage credentials. This means:

- S3 access keys hardcoded in Spark configs, DuckDB connection strings, Trino properties, and CI/CD pipelines.
- No way to scope access -- a key that can read `bronze.orders` can also read `gold.revenue`.
- No credential rotation without updating every application.
- No audit trail of who accessed what data.

Credential vending flips this model: the catalog server is the only component with permanent storage credentials. Clients get short-lived, scoped tokens on demand.

### How It Works in UC

Credential vending in UC was introduced in v0.2 ([release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.2.0)).

The flow:

1. **Client requests a table load** via the Iceberg REST Catalog API:
   ```
   GET /v1/namespaces/bronze/tables/orders
   ```

2. **UC validates the request.** If authorization is enabled, UC checks that the client's token has permission to access this table.

3. **UC generates scoped credentials.** Depending on the storage backend:
   - **AWS S3:** UC calls STS `AssumeRole` with a session policy scoped to the table's storage prefix, or generates a pre-signed URL set. The resulting credentials can only access the specific S3 prefix for that table.
   - **Azure ADLS:** UC generates a SAS token scoped to the container and path prefix.
   - **GCS:** UC generates a downscoped OAuth2 access token.
   - **S3-compatible (SeaweedFS/MinIO):** UC issues the configured access key with endpoint information. True STS-based scoping depends on the S3-compatible store's capabilities.

4. **UC returns the table metadata along with the credentials** in the `config` section of the response:

   ```json
   {
     "metadata-location": "s3://lakehouse/warehouse/bronze/orders/metadata/v3.metadata.json",
     "metadata": { "...": "..." },
     "config": {
       "s3.access-key-id": "ASIATEMP...",
       "s3.secret-access-key": "tempSecret...",
       "s3.session-token": "FwoGZX...",
       "s3.endpoint": "http://seaweedfs:8333",
       "s3.region": "us-east-1",
       "s3.path-style-access": "true"
     }
   }
   ```

5. **Client uses the vended credentials** to read Parquet files directly from object storage. The credentials expire after a configurable duration (default: 1 hour for STS tokens).

### Credential Scoping

| Storage | Scoping Mechanism | Granularity | True Scoping? |
|---------|-------------------|-------------|---------------|
| AWS S3 | STS AssumeRole with session policy | Per-prefix (table-level) | Yes |
| Azure ADLS | SAS token with path restriction | Per-container + prefix | Yes |
| GCS | Downscoped OAuth2 token | Per-bucket + prefix | Yes |
| SeaweedFS | Static key passthrough | Bucket-level | No (see note) |
| MinIO | STS via MinIO's STS endpoint | Per-prefix | Yes (if configured) |

**Note on SeaweedFS:** SeaweedFS does not implement the full AWS STS API. In lakehouse-stack, credential vending with SeaweedFS passes through the configured access key. This means all clients get the same level of access. For true per-table credential scoping, you need a storage backend that supports STS or equivalent (real AWS S3, MinIO with STS enabled, etc.).

### Configuration for Credential Vending

Credential vending is enabled automatically when storage credentials are configured in `server.properties`. No additional flag is needed.

For AWS S3 with true STS scoping, you also need:

```properties
# IAM role that UC will assume to generate scoped credentials
s3.assumeRoleArn.0=arn:aws:iam::123456789012:role/uc-credential-vending-role

# Duration for temporary credentials (seconds, default 3600)
s3.sessionDuration.0=3600
```

### Credential Vending vs Direct S3 Access

| Aspect | Direct S3 Keys | Credential Vending |
|--------|---------------|-------------------|
| **Key distribution** | Every client has permanent keys | Only UC has permanent keys |
| **Scope** | Full bucket access | Per-table prefix |
| **Rotation** | Manual, disruptive | Automatic (tokens expire) |
| **Audit** | S3 access logs only | UC access logs + S3 logs |
| **Revocation** | Delete/rotate the key | Tokens expire naturally; deny in UC for immediate |
| **Spark config** | `spark.hadoop.fs.s3a.access.key=...` | `spark.sql.catalog.iceberg.token=...` (UC handles the rest) |

---

## 6. Catalog-Managed Commits

### The Multi-Writer Problem

Iceberg's default commit model uses optimistic concurrency on the metadata file in object storage. When a writer wants to commit:

1. Read the current metadata file.
2. Create a new metadata file with the changes.
3. Atomically swap the metadata pointer (rename or conditional write).

This works well for a single writer. With multiple concurrent writers (e.g., Spark writing to the same table that DuckDB is also writing to), you get commit conflicts. The second writer's metadata pointer swap fails because the base metadata changed.

Iceberg handles this with retry-based conflict resolution, but it has limits:

- Writers must re-read metadata and retry.
- Under high contention, retries can cascade.
- Some storage backends (like S3) do not support true atomic rename, requiring a lock manager.

### Catalog-Managed Commits in UC

Catalog-managed commits, introduced as experimental in UC 0.3 ([release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.3.0)), move the commit coordination to the catalog server itself.

Instead of each writer independently updating metadata files in object storage, the flow becomes:

```
Writer A (Spark)                Unity Catalog                Writer B (DuckDB)
     │                              │                              │
     │  POST /v1/.../commit         │                              │
     │  {changes: [...]}            │                              │
     │ ───────────────────────────> │                              │
     │                              │  POST /v1/.../commit         │
     │                              │  {changes: [...]}            │
     │                              │ <─────────────────────────── │
     │                              │                              │
     │                              │  (UC serializes commits)     │
     │                              │  (applies A first, then B)   │
     │                              │                              │
     │  200 OK (committed)          │                              │
     │ <─────────────────────────── │                              │
     │                              │  200 OK (committed)          │
     │                              │ ───────────────────────────> │
```

The catalog server serializes commits, eliminating conflicts. This is the same model that AWS Glue uses for its Iceberg catalog and that the Iceberg REST spec supports via the `/v1/transactions/commit` endpoint.

### Current Status (0.3.1)

| Aspect | Status |
|--------|--------|
| Basic single-table commits | Works |
| Multi-writer coordination | Works for simple appends |
| Schema evolution during concurrent writes | Experimental, test thoroughly |
| Multi-table atomic commits | Not implemented |
| Conflict resolution strategy | Server-side serialization (no client retries needed) |
| Production readiness | **Experimental.** Use with caution. |

### Enabling Catalog-Managed Commits

As of UC 0.3.1, catalog-managed commits are automatically used when clients commit through the Iceberg REST Catalog API. There is no server-side toggle needed -- the REST protocol itself defines the commit path.

On the client side, when you use `RESTCatalog` as your catalog implementation, commits go through the REST API rather than directly to object storage. This is the default behavior for all engines using the Iceberg REST protocol.

```properties
# This is all you need -- commits go through the REST API automatically
spark.sql.catalog.iceberg.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.iceberg.uri             http://localhost:8080/api/2.1/unity-catalog/iceberg
```

### Caveats

- Catalog-managed commits add latency compared to direct object storage commits. Every commit requires a round-trip to the UC server.
- The UC server becomes a single point of failure for writes. If UC is down, no commits can succeed.
- For high-throughput streaming workloads with frequent micro-batch commits, test latency carefully.
- There is no built-in replication or HA for the UC server in OSS. Plan accordingly.

---

## 7. Multi-Engine Interop

This is the killer feature of UC OSS and the Iceberg REST Catalog protocol. Register a table once, read and write it from any engine.

### Spark

Spark uses the Iceberg REST Catalog via the `iceberg-spark-runtime` JAR.

**Configuration:**

```properties
spark.sql.extensions                      org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.catalog-impl    org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.iceberg.uri             http://localhost:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.iceberg.warehouse       unity
spark.sql.catalog.iceberg.token           not_used
```

**Usage:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UC-Spark") \
    .getOrCreate()

# Create schema and table
spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.bronze")
spark.sql("""
    CREATE TABLE iceberg.bronze.orders (
        order_id STRING,
        customer_id STRING,
        total DECIMAL(10,2),
        created_at TIMESTAMP
    ) USING ICEBERG
""")

# Insert data
spark.sql("""
    INSERT INTO iceberg.bronze.orders VALUES
    ('ord-001', 'cust-1', 99.99, current_timestamp()),
    ('ord-002', 'cust-2', 149.99, current_timestamp())
""")

# Query
spark.sql("SELECT * FROM iceberg.bronze.orders").show()
```

**Spark version compatibility:**

| Spark Version | Iceberg JAR | UC Compatibility |
|--------------|-------------|------------------|
| 4.0.x | `iceberg-spark-runtime-4.0_2.13-1.10.0.jar` | Full |
| 4.1.x | `iceberg-spark-runtime-4.0_2.13-1.10.0.jar` (same JAR) | Full |
| 3.5.x | `iceberg-spark-runtime-3.5_2.12-1.10.0.jar` | Full (REST protocol is version-independent) |

Reference: [Iceberg Spark documentation](https://iceberg.apache.org/docs/latest/spark-configuration/)

### DuckDB

DuckDB can read Iceberg tables via the `iceberg` extension, which supports the REST Catalog protocol natively since DuckDB 1.1.

**Using the Iceberg extension (recommended):**

```sql
-- Install and load
INSTALL iceberg;
LOAD iceberg;

-- Create a secret for the REST catalog
CREATE SECRET (
    TYPE ICEBERG_REST,
    ENDPOINT 'http://localhost:8080/api/2.1/unity-catalog/iceberg',
    TOKEN 'not_used',
    WAREHOUSE 'unity'
);

-- Attach the catalog
ATTACH '' AS unity (TYPE ICEBERG_REST);

-- Query tables
SELECT * FROM unity.bronze.orders LIMIT 10;

-- Aggregation
SELECT customer_id, SUM(total) as total_spend
FROM unity.bronze.orders
GROUP BY customer_id
ORDER BY total_spend DESC;
```

**Using the UC-specific extension (legacy):**

DuckDB also provides a `uc_catalog` extension that uses the UC Native API rather than the Iceberg REST API:

```sql
INSTALL uc_catalog FROM core_nightly;
LOAD uc_catalog;

CREATE SECRET (
    TYPE UC,
    TOKEN 'not_used',
    ENDPOINT 'http://127.0.0.1:8080',
    AWS_REGION 'us-east-1'
);

ATTACH 'unity' AS unity (TYPE UC_CATALOG);
SELECT * FROM unity.bronze.orders;
```

The `iceberg` extension approach is preferred because it uses the standard Iceberg REST protocol and gets credential vending automatically. The `uc_catalog` extension predates REST Catalog support in DuckDB and requires separate S3 credential configuration.

Reference: [DuckDB Iceberg extension](https://duckdb.org/docs/extensions/iceberg.html)

### Trino

Trino supports Iceberg tables via a REST catalog since Trino 405.

**Catalog configuration** (`etc/catalog/lakehouse.properties`):

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:8080/api/2.1/unity-catalog/iceberg
iceberg.rest-catalog.warehouse=unity
```

**Usage:**

```sql
-- List schemas
SHOW SCHEMAS FROM lakehouse;

-- Query
SELECT * FROM lakehouse.bronze.orders LIMIT 10;

-- Time travel
SELECT * FROM lakehouse.bronze.orders FOR VERSION AS OF 1234567890;
```

Reference: [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)

### Polars

Polars supports reading Iceberg tables via the `pyiceberg` library, which speaks the REST Catalog protocol.

```python
import polars as pl
from pyiceberg.catalog import load_catalog

# Connect to UC via Iceberg REST protocol
catalog = load_catalog(
    "unity",
    type="rest",
    uri="http://localhost:8080/api/2.1/unity-catalog/iceberg",
    warehouse="unity",
    token="not_used",
)

# Load table
table = catalog.load_table("bronze.orders")

# Convert to Polars DataFrame
df = pl.from_arrow(table.scan().to_arrow())
print(df)

# Or use scan with predicates
df = pl.from_arrow(
    table.scan(
        row_filter="total > 100.00",
        selected_fields=("order_id", "customer_id", "total"),
    ).to_arrow()
)
```

Reference: [PyIceberg documentation](https://py.iceberg.apache.org/)

### Dremio

Dremio supports Iceberg REST catalogs as a data source.

**Configuration in Dremio UI or API:**

```json
{
  "entityType": "source",
  "type": "ICEBERG_REST",
  "name": "lakehouse",
  "config": {
    "uri": "http://localhost:8080/api/2.1/unity-catalog/iceberg",
    "warehouse": "unity",
    "properties": {
      "token": "not_used"
    }
  }
}
```

Reference: [Dremio Iceberg REST Catalog](https://docs.dremio.com/current/sonar/data-sources/iceberg-rest/)

### PyIceberg (Python Native)

For Python applications that want direct Iceberg access without Spark:

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "unity",
    type="rest",
    uri="http://localhost:8080/api/2.1/unity-catalog/iceberg",
    warehouse="unity",
    token="not_used",
)

# List namespaces
for ns in catalog.list_namespaces():
    print(f"Namespace: {ns}")

# List tables
for table_id in catalog.list_tables("bronze"):
    print(f"Table: {table_id}")

# Load and scan a table
table = catalog.load_table("bronze.orders")
scan = table.scan(limit=100)
df = scan.to_pandas()
print(df)

# Table metadata
print(f"Schema: {table.schema()}")
print(f"Snapshots: {table.metadata.snapshots}")
print(f"Current snapshot: {table.metadata.current_snapshot_id}")
```

Reference: [PyIceberg REST Catalog](https://py.iceberg.apache.org/configuration/#rest-catalog)

### Engine Compatibility Matrix

| Engine | Read | Write | Time Travel | Schema Evolution | Credential Vending | Notes |
|--------|------|-------|-------------|-----------------|-------------------|-------|
| **Spark 4.x** | Yes | Yes | Yes | Yes | Yes | Full support via `iceberg-spark-runtime` |
| **Spark 3.5** | Yes | Yes | Yes | Yes | Yes | Requires `iceberg-spark-runtime-3.5` |
| **DuckDB 1.1+** | Yes | No | Yes | N/A | Yes | Read-only via REST; write support planned |
| **Trino 405+** | Yes | Yes | Yes | Yes | Yes | Full read/write via `iceberg` connector |
| **Polars** | Yes | No | Yes | N/A | Yes | Read via PyIceberg; write via PyIceberg planned |
| **Dremio** | Yes | Yes | Yes | Yes | Yes | Full support |
| **PyIceberg** | Yes | Yes | Yes | Yes | Yes | Python-native, no JVM needed |
| **Flink 1.18+** | Yes | Yes | Yes | Yes | Yes | Via Iceberg Flink connector |

---

## 8. Governance Capabilities

### What Exists Today (UC 0.3.1)

UC OSS governance is minimal compared to managed Databricks UC. Here is an honest accounting of what is and is not available.

#### Token-Based Authentication

UC supports bearer token authentication. When `authorization.enabled=true` in `server.properties`, all API requests must include a valid token in the `Authorization` header.

```bash
# With auth enabled
curl -H "Authorization: Bearer your_token_here" \
  http://localhost:8080/api/2.1/unity-catalog/catalogs
```

In the current implementation, tokens are validated against a configured set of allowed tokens. There is no OAuth2/OIDC flow built in -- you generate tokens and configure them manually.

#### Basic Access Control

UC 0.3.1 supports basic ownership-based access control:

- **Catalog ownership:** The creator of a catalog is its owner. Owners can create, modify, and delete schemas within the catalog.
- **Schema ownership:** The creator of a schema can manage tables within it.
- **No GRANT/REVOKE:** There is no SQL-level `GRANT` or `REVOKE` syntax. Access is all-or-nothing based on token validity.

#### What This Means in Practice

For a small team running a self-hosted lakehouse:

- Token auth prevents anonymous access.
- You can issue different tokens to different applications or users.
- **But:** any valid token can access any catalog, schema, and table. There is no fine-grained permission model.

For multi-team environments, this is insufficient. You would need to implement access control at the network layer (firewall rules, reverse proxy with path-based routing) or wait for the RBAC implementation planned for v0.5.

### Governance Feature Comparison

| Feature | Databricks UC (Managed) | UC OSS 0.3.1 |
|---------|------------------------|---------------|
| Three-level namespace | Yes | Yes |
| Token authentication | Yes (OAuth2/OIDC) | Yes (bearer token, manual) |
| RBAC (GRANT/REVOKE) | Yes | No (planned v0.5) |
| Row-level security | Yes | No |
| Column-level masking | Yes | No |
| Audit logging | Yes (system tables) | No |
| Data lineage | Yes (system tables) | No |
| Data classification | Yes (automatic PII detection) | No |
| Attribute-based access control | Yes | No |
| IP access lists | Yes | No |
| Encryption at rest (managed) | Yes | No (delegated to storage) |
| Delta Sharing | Yes | No |
| Lakehouse Federation | Yes | No |
| Workspace binding | Yes | N/A (no workspace concept) |

---

## 9. Model Registry and MLflow Integration

### Overview

Since UC 0.2, Unity Catalog OSS supports a registered model registry that integrates with MLflow. This allows you to register, version, and serve ML models with the same catalog that manages your data tables.

Reference: [UC 0.2 release notes](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.2.0)

### How It Works

UC stores model metadata (name, version, description, source path) in its metadata store. Model artifacts (the actual model files) are stored in object storage, referenced by the registered model's `storage_location`.

MLflow 2.16.1+ includes a Unity Catalog client that can use UC OSS as a model registry backend.

```
┌──────────────┐         ┌──────────────────┐         ┌──────────────┐
│   MLflow     │  REST   │  Unity Catalog   │  S3 API │  SeaweedFS   │
│  Client /    │────────>│  Server          │────────>│  (or S3)     │
│  Tracking    │         │                  │         │              │
│  Server      │         │  /models         │         │  Model       │
│              │         │  /model-versions │         │  Artifacts   │
└──────────────┘         └──────────────────┘         └──────────────┘
```

### Setup

**Step 1: Configure MLflow to use UC as model registry.**

```bash
# Set the MLflow model registry URI to point to UC
export MLFLOW_REGISTRY_URI="uc:http://localhost:8080"
```

Or in Python:

```python
import mlflow

mlflow.set_registry_uri("uc:http://localhost:8080")
```

**Step 2: Register a model.**

```python
import mlflow
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris

mlflow.set_registry_uri("uc:http://localhost:8080")

# Train a model
X, y = load_iris(return_X_y=True)
model = RandomForestClassifier(n_estimators=100)
model.fit(X, y)

# Log and register in UC
with mlflow.start_run():
    signature = infer_signature(X, model.predict(X))
    mlflow.sklearn.log_model(
        model,
        "iris_model",
        signature=signature,
        registered_model_name="unity.default.iris_classifier",
    )
```

**Step 3: Load a registered model.**

```python
import mlflow

mlflow.set_registry_uri("uc:http://localhost:8080")

# Load the latest version
model = mlflow.pyfunc.load_model("models:/unity.default.iris_classifier/1")
predictions = model.predict(X_test)
```

### UC Native API for Models

```bash
# List registered models
curl http://localhost:8080/api/2.1/unity-catalog/models?catalog_name=unity&schema_name=default

# Get model details
curl http://localhost:8080/api/2.1/unity-catalog/models/unity.default.iris_classifier

# List model versions
curl "http://localhost:8080/api/2.1/unity-catalog/model-versions?full_name=unity.default.iris_classifier"
```

### MLflow Version Requirements

| Feature | Minimum MLflow Version | Notes |
|---------|----------------------|-------|
| UC model registry (basic) | 2.16.1 | Initial integration |
| UC model registry (improved) | 2.17.0 | Bug fixes, better error handling |
| MLflow 3.x compatibility | 3.0.0+ | Works with UC 0.3.x |
| Model serving from UC | 2.17.0+ | Load models registered in UC |

Reference: [MLflow Unity Catalog integration](https://mlflow.org/docs/latest/plugins.html#unity-catalog)

### Limitations

- **No model lineage.** UC OSS does not track which datasets were used to train a model.
- **No model serving endpoint management.** UC stores model metadata and artifacts; it does not serve predictions.
- **No A/B testing or champion/challenger routing.** These are Databricks-managed features.
- **No automatic model validation.** Model quality gates must be implemented in your MLflow workflow.

---

## 10. The Honest Gaps vs Databricks UC

This section is the reason this guide exists. Databricks users evaluating UC OSS deserve an honest accounting of what they will and will not get.

### What UC OSS Gives You

These features are production-ready (or close to it) in UC 0.3.1:

| Feature | Quality | Notes |
|---------|---------|-------|
| **Iceberg REST Catalog** | Production-ready | The strongest feature. Spec-compliant, well-tested, battle-tested by early adopters. |
| **Credential vending** | Production-ready (AWS/Azure/GCS) | Works well with real cloud storage. Limited with S3-compatible stores (SeaweedFS/MinIO) unless they support STS. |
| **Multi-engine interop** | Production-ready | Spark, DuckDB, Trino, Polars, Dremio, PyIceberg all work. This is the primary value proposition. |
| **Three-level namespace** | Production-ready | `catalog.schema.table` works exactly as expected. |
| **Model registry** | Stable | Works with MLflow 2.16.1+. Basic but functional. |
| **Volumes** | Stable | Managed and external volumes for non-tabular data. |
| **Functions** | Stable | User-defined function registration (metadata only). |
| **Catalog-managed commits** | Experimental | Works for basic cases. Not production-tested at scale. |
| **PostgreSQL backend** | Stable (since 0.3) | Replaces H2 for production metadata persistence. |

### What UC OSS Does NOT Give You

These are features of Databricks managed UC that are **absent** from UC OSS 0.3.1. Some are on the roadmap; some are not.

#### No Full RBAC (Planned for v0.5)

Databricks UC has a complete SQL-level permission model:

```sql
-- This works in Databricks UC:
GRANT SELECT ON TABLE bronze.orders TO `data-analysts`;
GRANT CREATE TABLE ON SCHEMA silver TO `data-engineers`;
REVOKE ALL PRIVILEGES ON CATALOG production FROM `interns`;
```

UC OSS does not implement `GRANT` or `REVOKE`. Access control is all-or-nothing: if you have a valid token, you can access everything.

**Roadmap:** RBAC is the most-requested feature and is targeted for v0.5. The UC project has a [design proposal](https://github.com/unitycatalog/unitycatalog/discussions) for privilege management, but no implementation has merged as of March 2026.

**Workaround:** Use network-level access control (reverse proxy with path-based routing, API gateway) to restrict access by application or team. This is coarse-grained but functional.

#### No Row-Level Security

Databricks UC supports row filters:

```sql
-- Databricks UC only:
ALTER TABLE bronze.orders SET ROW FILTER filter_by_region ON (region);
```

UC OSS has no row-level filtering. Every query sees all rows.

**Workaround:** Implement row filtering in application-level views or materialized tables (create per-team views that include `WHERE` clauses).

#### No Column-Level Security

Databricks UC supports column masking:

```sql
-- Databricks UC only:
ALTER TABLE bronze.customers
ALTER COLUMN ssn SET MASK mask_ssn;
```

UC OSS has no column masking. Sensitive columns are visible to all authorized users.

**Workaround:** Create separate views that exclude or hash sensitive columns, and direct users to those views.

#### No Audit Logging

Databricks UC logs every access event to system tables (`system.access.audit`). You can query who accessed what table, when, and from which IP address.

UC OSS has no built-in audit logging. Server logs record HTTP requests, but there is no structured, queryable audit trail.

**Workaround:** Place a reverse proxy (nginx, Envoy) in front of UC and log all requests. Parse the access logs into a structured format. This gives you basic "who called what endpoint" auditing but not the rich metadata (query text, rows read, etc.) that Databricks provides.

#### No Data Lineage

Databricks UC tracks table-level lineage automatically: which tables read from which other tables, which notebooks produce which outputs, column-level lineage for views.

UC OSS has no lineage tracking.

**Workaround:** Use external lineage tools (OpenLineage, Marquez, DataHub) with Spark's OpenLineage integration to capture lineage events:

```properties
# Spark config for OpenLineage
spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://marquez:5000
```

Reference: [OpenLineage Spark integration](https://openlineage.io/docs/integrations/spark/)

#### No Delta Sharing

Databricks UC supports Delta Sharing -- a protocol for securely sharing data across organizations without copying it.

UC OSS does not implement Delta Sharing. If you need to share data externally, you must manage access through traditional methods (shared S3 buckets with IAM, pre-signed URLs, or data export).

Reference: [Delta Sharing specification](https://github.com/delta-io/delta-sharing)

#### No Lakehouse Federation

Databricks UC can federate queries across external databases (PostgreSQL, MySQL, Snowflake, etc.) without moving data.

UC OSS is a catalog for Iceberg (and Delta/Hudi) tables only. It does not proxy queries to external databases.

**Workaround:** Use Spark's JDBC data source or Trino's connector ecosystem to query external databases directly. This is not catalog-managed federation, but it achieves the same result.

#### No Proactive Data Classification

Databricks UC can automatically detect PII and sensitive data in tables and apply tags.

UC OSS has no data classification capabilities.

#### No Multi-Workspace Management

Databricks UC supports workspace binding -- controlling which workspaces can access which catalogs and data assets.

UC OSS has no workspace concept. A single UC server manages all catalogs directly.

### Summary Table

| Capability | Databricks UC | UC OSS 0.3.1 | Gap Severity |
|-----------|---------------|---------------|--------------|
| Iceberg REST Catalog | Yes | Yes | None |
| Delta Lake tables | Yes | Yes | None |
| Credential vending | Yes | Yes | None (cloud); Partial (S3-compat) |
| Multi-engine interop | Yes (with restrictions) | Yes | None |
| Full RBAC | Yes | No | **High** (planned v0.5) |
| Row-level security | Yes | No | High |
| Column masking | Yes | No | High |
| Audit logging | Yes | No | **High** |
| Data lineage | Yes | No | Medium |
| Delta Sharing | Yes | No | Medium |
| Lakehouse Federation | Yes | No | Medium |
| Data classification | Yes | No | Low |
| Model registry | Yes | Yes (basic) | Low |
| Multi-workspace | Yes | N/A | Low (different architecture) |
| Volumes | Yes | Yes | None |
| Functions | Yes | Yes | None |
| Views | Yes | Partial | Low |

### The Honest Take

UC OSS is a **catalog + credential vending + interop layer**. It is not a governance platform.

For a small team (1-10 engineers) running a self-hosted lakehouse, UC OSS provides genuine value: one catalog for all engines, credential vending instead of scattered S3 keys, and the Iceberg REST standard that prevents lock-in.

For a larger organization that needs audit logging, RBAC, row/column-level security, and compliance reporting, UC OSS is not there yet. You need either Databricks managed UC, or you need to build the governance layer yourself on top of UC OSS.

That is not a failure of the project. It is the reality of open source vs managed services. Governance is a vertical integration problem that requires deep coupling with identity providers, storage backends, and compute runtimes. A standalone open-source server cannot realistically provide all of that.

**What you get:** The foundation. A solid catalog with the right protocol (Iceberg REST) and the right architecture (credential vending, multi-engine).

**What you build or buy:** The governance layer on top.

---

## 11. Version History and Roadmap

### Release History

#### v0.1 (June 2024)

**Theme:** Initial open-source release.

- Three-level namespace: catalog, schema, table.
- UC Native REST API (`/api/2.1/unity-catalog/`).
- Delta Lake table support.
- Iceberg table support (basic).
- Embedded H2 database for metadata.
- CLI (`bin/uc`) for catalog management.
- Docker image on Docker Hub.

Reference: [v0.1 release](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.1.0)

#### v0.2 (October 2024)

**Theme:** Credential vending, MLflow, DuckDB.

- **Credential vending** for S3, Azure ADLS, and GCS. The catalog now issues short-lived storage credentials instead of requiring clients to have permanent keys.
- **Iceberg REST Catalog API** (`/api/2.1/unity-catalog/iceberg/v1/`). Engines can now use the standard Iceberg REST protocol, not just the UC-specific API.
- **MLflow model registry integration.** Register and version ML models in UC. Requires MLflow 2.16.1+.
- **DuckDB `uc_catalog` extension.** Query UC tables from DuckDB.
- **Volume support.** Managed and external volumes for non-tabular data (files, images, etc.).
- **Function registration.** Register UDFs in the catalog (metadata only).
- Bug fixes and performance improvements.

Reference: [v0.2 release](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.2.0)

#### v0.3 (May 2025)

**Theme:** Production hardening, catalog-managed commits.

- **PostgreSQL backend.** Use PostgreSQL or MySQL instead of embedded H2 for production metadata persistence. Configured via Hibernate properties in `server.properties`.
- **Catalog-managed commits** (experimental). UC coordinates Iceberg table commits centrally, enabling safe multi-engine writes.
- **Improved Iceberg REST spec compliance.** Better handling of namespace properties, table requirements, and scan metrics reporting.
- **Helm chart** for Kubernetes deployment.
- **UC Web UI** (optional companion container). Graphical interface for browsing catalogs, schemas, and tables.
- Token-based authorization framework (basic).
- Performance improvements for credential vending.

Reference: [v0.3 release](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.3.0)

#### v0.3.1 (February 2026)

**Theme:** Stability, Iceberg 1.10 compatibility.

- Bug fixes for credential vending with non-standard S3 endpoints.
- Compatibility fixes for Iceberg 1.10 metadata format changes.
- Improved error messages in REST API responses.
- Docker image size optimization.
- Documentation updates.

This is the version used in lakehouse-stack.

Reference: [v0.3.1 release](https://github.com/unitycatalog/unitycatalog/releases/tag/v0.3.1)

### Roadmap

Based on public GitHub discussions and LF AI & Data governance meetings:

| Version | Target | Key Features |
|---------|--------|--------------|
| v0.4 | Mid 2026 | Improved view support, table maintenance operations (compaction, orphan file cleanup via API), enhanced token management |
| v0.5 | Late 2026 | **RBAC with GRANT/REVOKE** (the most-requested feature), audit log framework, improved multi-tenant support |
| v0.6+ | 2027 | Data lineage (basic), Delta Sharing integration (under discussion), column-level access control |

**Disclaimer:** Roadmap items are based on community discussions and are subject to change. UC is a Linux Foundation project with multiple contributors; features are delivered when contributors implement them.

Reference: [UC GitHub Discussions](https://github.com/unitycatalog/unitycatalog/discussions)

---

## 12. Integration with lakehouse-stack

### Architecture in lakehouse-stack

```
┌──────────────────────────────────────────────────────────────────┐
│                     lakehouse-stack                                │
│                                                                    │
│   ┌──────────────────────────────────────────────────────────┐   │
│   │                  Spark 4.1 Cluster                        │   │
│   │  spark-master-41:7078  (UI: 8082)                        │   │
│   │                                                          │   │
│   │  Catalog config (pick one):                              │   │
│   │    A) JDBC → PostgreSQL (default, no UC needed)          │   │
│   │    B) REST → Unity Catalog OSS (this guide)              │   │
│   └──────────────┬───────────────┬───────────────────────────┘   │
│                   │               │                                │
│          (option A)│      (option B)│                                │
│                   │               │                                │
│   ┌───────────────▼──┐   ┌───────▼───────────────────────────┐   │
│   │   PostgreSQL     │   │   Unity Catalog OSS 0.3.1        │   │
│   │   :5432          │   │   :8080                           │   │
│   │                  │   │                                   │   │
│   │   iceberg_catalog│   │   REST API + Iceberg REST Catalog│   │
│   │   (JDBC catalog) │   │   + Credential Vending           │   │
│   └──────────────────┘   └───────────────┬───────────────────┘   │
│                                           │                        │
│                                   Credential Vending               │
│                                           │                        │
│   ┌───────────────────────────────────────▼──────────────────┐   │
│   │              SeaweedFS (S3-compatible)                     │   │
│   │              :8333                                        │   │
│   │              s3://lakehouse/warehouse/                     │   │
│   │                                                          │   │
│   │   bronze/orders/      → Iceberg table data + metadata    │   │
│   │   silver/orders/      → Cleaned data                     │   │
│   │   gold/revenue/       → Aggregated metrics               │   │
│   └──────────────────────────────────────────────────────────┘   │
│                                                                    │
│   Other consumers (with UC):                                       │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│   │ DuckDB   │  │ Trino    │  │ PyIceberg│                      │
│   │ (laptop) │  │ (server) │  │ (Python) │                      │
│   └──────────┘  └──────────┘  └──────────┘                      │
│   All connect to UC at :8080 via Iceberg REST protocol            │
│                                                                    │
└──────────────────────────────────────────────────────────────────┘
```

### Two Catalog Options

lakehouse-stack supports two catalog backends. You choose one:

| | PostgreSQL JDBC (Default) | Unity Catalog OSS |
|---|---|---|
| **Config file** | `config/spark/spark-defaults.conf` (from `.conf.example`) | `config/spark/spark-defaults.conf` (from `spark-defaults-uc.conf.example`) |
| **Docker Compose** | `docker-compose-spark41.yml` only | `docker-compose-spark41.yml` + `docker-compose-unity-catalog.yml` |
| **Start command** | `./lakehouse start all` | `./lakehouse start all && ./lakehouse start unity-catalog` |
| **Multi-engine** | No (Spark only) | Yes (Spark, DuckDB, Trino, etc.) |
| **Credential vending** | No (hardcoded S3 keys in Spark config) | Yes (UC issues credentials) |
| **Complexity** | Lower (one fewer service) | Higher (UC server to manage) |
| **When to use** | Single-engine (Spark only), simplicity | Multi-engine, credential management, governance path |

### Switching from JDBC to UC

**Step 1: Start UC.**

```bash
./lakehouse start unity-catalog
```

**Step 2: Swap Spark config.**

```bash
cp config/spark/spark-defaults-uc.conf.example config/spark/spark-defaults.conf
# Edit with your SeaweedFS credentials if needed
```

**Step 3: Restart Spark.**

```bash
./lakehouse stop all
./lakehouse start all
```

**Step 4: Migrate existing tables.**

Tables created with the JDBC catalog are not automatically visible in UC. You need to register them:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Migrate").getOrCreate()

# Read from existing Iceberg table (data is still in SeaweedFS)
# Register the table location in UC
spark.sql("""
    CREATE TABLE iceberg.bronze.orders
    USING ICEBERG
    LOCATION 's3://lakehouse/warehouse/bronze/orders'
""")
```

Since both the JDBC catalog and UC point to the same SeaweedFS storage, the data files do not move. You are only registering the existing metadata location in UC.

### Running Both Catalogs Side-by-Side

For migration or testing, you can configure Spark with both catalogs simultaneously:

```properties
# JDBC catalog (existing)
spark.sql.catalog.jdbc_iceberg           org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.jdbc_iceberg.type      jdbc
spark.sql.catalog.jdbc_iceberg.uri       jdbc:postgresql://localhost:5432/iceberg_catalog
spark.sql.catalog.jdbc_iceberg.jdbc.user postgres
spark.sql.catalog.jdbc_iceberg.jdbc.password your_password

# Unity Catalog (new)
spark.sql.catalog.unity                  org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.unity.catalog-impl     org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.unity.uri              http://localhost:8080/api/2.1/unity-catalog/iceberg
spark.sql.catalog.unity.warehouse        unity
spark.sql.catalog.unity.token            not_used
```

Then copy data between catalogs:

```python
# Read from JDBC catalog
df = spark.table("jdbc_iceberg.bronze.orders")

# Write to UC catalog
df.writeTo("unity.bronze.orders").createOrReplace()
```

### SeaweedFS Considerations

SeaweedFS is an S3-compatible object store, but it does not implement the full AWS API surface. Key limitations when used with UC:

| AWS S3 Feature | SeaweedFS Support | Impact on UC |
|----------------|------------------|--------------|
| Basic S3 API (GET/PUT/DELETE) | Yes | None |
| Multipart upload | Yes | None |
| ListObjectsV2 | Yes | None |
| STS AssumeRole | No | Credential vending passes through static keys (no per-table scoping) |
| S3 Select | No | No impact (UC does not use S3 Select) |
| Server-side encryption (SSE-S3) | Partial | Data at rest encryption must be handled at filesystem level |

For production environments requiring per-table credential scoping, consider placing MinIO (with STS support) or real AWS S3 behind UC.

### Port Allocation

When running both the default stack and UC:

| Service | Port | Notes |
|---------|------|-------|
| PostgreSQL | 5432 | Metadata store (JDBC catalog and/or UC backend) |
| SeaweedFS | 8333 | Object storage |
| Spark Master 4.1 | 7078 | Spark cluster manager |
| Spark UI 4.1 | 8082 | Web UI |
| **Unity Catalog** | **8080** | REST API + Iceberg REST Catalog |
| Unity Catalog UI (optional) | 3000 | Web-based catalog browser |
| Kafka | 9092 | Streaming |
| Airflow | 8085 | Orchestration |

Note: if you also run the UC Web UI, it occupies port 3000. Uncomment the `unity-catalog-ui` service in `docker-compose-unity-catalog.yml` to enable it.

---

## 13. CLI Reference

UC ships with a command-line client (`bin/uc`) for managing catalog objects. This is useful for scripting, CI/CD, and quick exploration.

### Installation

The CLI is included in the UC binary distribution:

```bash
# If using binary install
cd unitycatalog-0.3.1
bin/uc --help

# If using Docker, exec into the container
docker exec -it unity-catalog bin/uc --help
```

### Catalog Commands

```bash
# List catalogs
bin/uc catalog list --server http://localhost:8080

# Create a catalog
bin/uc catalog create --name production --server http://localhost:8080

# Get catalog details
bin/uc catalog get --name unity --server http://localhost:8080

# Delete a catalog
bin/uc catalog delete --name test_catalog --server http://localhost:8080
```

### Schema Commands

```bash
# List schemas in a catalog
bin/uc schema list --catalog unity --server http://localhost:8080

# Create a schema
bin/uc schema create --catalog unity --name staging \
    --comment "Staging area for data validation" \
    --server http://localhost:8080

# Get schema details
bin/uc schema get --full_name unity.bronze --server http://localhost:8080

# Delete a schema
bin/uc schema delete --full_name unity.staging --server http://localhost:8080
```

### Table Commands

```bash
# List tables in a schema
bin/uc table list --catalog unity --schema bronze --server http://localhost:8080

# Create an external Iceberg table
bin/uc table create --full_name unity.bronze.events \
    --columns "event_id STRING, event_type STRING, timestamp TIMESTAMP, payload STRING" \
    --storage_location s3://lakehouse/warehouse/bronze/events \
    --format ICEBERG \
    --server http://localhost:8080

# Get table details
bin/uc table get --full_name unity.bronze.orders --server http://localhost:8080

# Read table data (small tables only, for debugging)
bin/uc table read --full_name unity.bronze.orders --server http://localhost:8080

# Delete a table
bin/uc table delete --full_name unity.bronze.events --server http://localhost:8080
```

### Volume Commands

```bash
# List volumes
bin/uc volume list --catalog unity --schema bronze --server http://localhost:8080

# Create a volume
bin/uc volume create --full_name unity.bronze.raw_files \
    --storage_location s3://lakehouse/warehouse/bronze/raw_files \
    --server http://localhost:8080

# Get volume details
bin/uc volume get --full_name unity.bronze.raw_files --server http://localhost:8080

# Delete a volume
bin/uc volume delete --full_name unity.bronze.raw_files --server http://localhost:8080
```

### Function Commands

```bash
# List functions
bin/uc function list --catalog unity --schema bronze --server http://localhost:8080

# Create a function
bin/uc function create --full_name unity.bronze.to_upper \
    --input_params "s STRING" \
    --data_type STRING \
    --comment "Convert string to uppercase" \
    --server http://localhost:8080

# Get function details
bin/uc function get --full_name unity.bronze.to_upper --server http://localhost:8080

# Delete a function
bin/uc function delete --full_name unity.bronze.to_upper --server http://localhost:8080
```

### Model Commands

```bash
# List registered models
bin/uc model list --catalog unity --schema default --server http://localhost:8080

# Create a registered model
bin/uc model create --full_name unity.default.revenue_predictor \
    --comment "Revenue prediction model" \
    --server http://localhost:8080

# Get model details
bin/uc model get --full_name unity.default.revenue_predictor --server http://localhost:8080

# List model versions
bin/uc model_version list --full_name unity.default.revenue_predictor \
    --server http://localhost:8080

# Delete a model
bin/uc model delete --full_name unity.default.revenue_predictor --server http://localhost:8080
```

### Common CLI Patterns

```bash
# Export catalog inventory to JSON
bin/uc catalog list --server http://localhost:8080 --output json > catalogs.json
bin/uc schema list --catalog unity --server http://localhost:8080 --output json > schemas.json

# Script: create medallion schemas
for layer in bronze silver gold; do
    bin/uc schema create --catalog unity --name $layer \
        --comment "Medallion $layer layer" \
        --server http://localhost:8080
done

# Health check in CI/CD
if bin/uc catalog list --server http://localhost:8080 > /dev/null 2>&1; then
    echo "UC is healthy"
else
    echo "UC is unreachable"
    exit 1
fi
```

### lakehouse-stack CLI Integration

The lakehouse-stack CLI wraps Docker Compose operations for UC:

```bash
# Start Unity Catalog
./lakehouse start unity-catalog
# Runs: docker compose -f docker-compose-unity-catalog.yml up -d

# Stop Unity Catalog
./lakehouse stop unity-catalog
# Runs: docker compose -f docker-compose-unity-catalog.yml down

# View UC logs
./lakehouse logs unity-catalog
# Runs: docker logs unity-catalog

# Check status (includes UC if running)
./lakehouse status

# Full connectivity test (includes UC if running)
./lakehouse test
```

---

## 14. Troubleshooting

### UC Server Will Not Start

**Symptom:** Container exits immediately or health check fails.

```bash
# Check container logs
docker logs unity-catalog

# Check if the port is already in use
ss -tlnp | grep 8080

# Check if the config file exists and is valid
docker exec unity-catalog cat /opt/unitycatalog/etc/conf/server.properties
```

**Common causes:**

| Cause | Fix |
|-------|-----|
| Port 8080 already in use | Stop the conflicting service or change `server.port` in `server.properties` |
| Invalid `server.properties` | Check for syntax errors; properties file uses `key=value` format, no quotes |
| Missing config volume mount | Ensure `./config/unity-catalog` exists and contains `server.properties` |
| Java OOM | Increase `JAVA_OPTS=-Xmx2g` in `docker-compose-unity-catalog.yml` |
| H2 database corruption | Delete the `uc-data` Docker volume: `docker volume rm lakehouse-stack_uc-data` |

### Spark Cannot Connect to UC

**Symptom:** Spark throws `org.apache.iceberg.exceptions.RESTException` or connection refused.

```bash
# Verify UC is running and healthy
curl -s http://localhost:8080/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Verify Spark config is correct
docker exec spark-master-41 cat /opt/spark/conf/spark-defaults.conf | grep catalog
```

**Common causes:**

| Cause | Fix |
|-------|-----|
| UC not running | `./lakehouse start unity-catalog` |
| Wrong URI in Spark config | Must be `http://localhost:8080/api/2.1/unity-catalog/iceberg` (not just `http://localhost:8080`) |
| Network isolation (Docker) | Use `http://unity-catalog:8080` if Spark is in the same Docker network; use `http://localhost:8080` if Spark accesses UC via host port mapping |
| Missing `iceberg-spark-runtime` JAR | Verify JARs are present: `docker exec spark-master-41 ls /opt/spark/jars-extra/` |
| Token auth enabled but no token in Spark config | Add `spark.sql.catalog.iceberg.token=your_token` to Spark config |

### Tables Not Visible

**Symptom:** `SHOW TABLES` returns empty, or table queries fail with "table not found".

```bash
# Check via REST API — are the schemas there?
curl -s "http://localhost:8080/api/2.1/unity-catalog/schemas?catalog_name=unity" | python3 -m json.tool

# Check via REST API — are the tables there?
curl -s "http://localhost:8080/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=bronze" | python3 -m json.tool
```

**Common causes:**

| Cause | Fix |
|-------|-----|
| Tables registered in JDBC catalog, not UC | Register tables in UC (see Section 12, migration) |
| Wrong warehouse name in Spark config | `spark.sql.catalog.iceberg.warehouse` must match the catalog name in UC (default: `unity`) |
| Schema does not exist in UC | Create it: `curl -X POST http://localhost:8080/api/2.1/unity-catalog/schemas -H 'Content-Type: application/json' -d '{"name":"bronze","catalog_name":"unity"}'` |
| Using wrong catalog prefix in SQL | Use `iceberg.bronze.orders` (not `unity.bronze.orders`) if your Spark catalog is named `iceberg` |

### Credential Vending Failures

**Symptom:** Table metadata loads correctly, but reading data files fails with access denied.

```bash
# Test direct S3 access from Spark container
docker exec spark-master-41 hadoop fs -ls s3a://lakehouse/warehouse/
```

**Common causes:**

| Cause | Fix |
|-------|-----|
| S3 credentials wrong in `server.properties` | Verify `s3.accessKey.0` and `s3.secretKey.0` |
| S3 endpoint wrong | For SeaweedFS: `s3.endpoint.0=http://seaweedfs:8333` (Docker network name) or `http://localhost:8333` (host access) |
| SeaweedFS not running | `./lakehouse status` to check |
| Bucket does not exist in SeaweedFS | Create it: `aws --endpoint-url http://localhost:8333 s3 mb s3://lakehouse` |
| Path style access not configured | SeaweedFS requires path-style access. Ensure `s3.path-style-access` is set if your client configuration supports it. |

### DuckDB Cannot Read Tables

**Symptom:** DuckDB throws errors when querying UC tables.

**Common causes:**

| Cause | Fix |
|-------|-----|
| DuckDB version too old | Need DuckDB 1.1+ for Iceberg REST support |
| Wrong extension | Use `iceberg` extension (not `uc_catalog`) for REST Catalog |
| Endpoint URL wrong | Use `http://localhost:8080/api/2.1/unity-catalog/iceberg` |
| SeaweedFS inaccessible from host | DuckDB runs on the host; SeaweedFS must be accessible at `localhost:8333` |
| SSL issues | If using `https`, ensure certificates are valid or disable verification |

### Performance Issues

**Symptom:** Queries are slow when using UC compared to direct JDBC catalog.

| Issue | Cause | Mitigation |
|-------|-------|------------|
| Slow table loads | Each table load makes an HTTP round-trip to UC | Normal; the overhead is typically <100ms per table load |
| Slow commits | Catalog-managed commits add a round-trip per commit | For high-frequency streaming, consider the JDBC catalog or batch commits |
| UC server high CPU | Too many concurrent table load requests | Increase `-Xmx` in `JAVA_OPTS`; consider running multiple UC replicas behind a load balancer (stateless if using PostgreSQL backend) |
| Metadata store slow | H2 under load | Switch to PostgreSQL backend (see `server.properties` configuration) |

### Recovering from H2 Database Corruption

The default embedded H2 database can become corrupted if the UC container is killed abruptly.

```bash
# Stop UC
./lakehouse stop unity-catalog

# Remove the data volume (WARNING: this deletes all UC metadata)
docker volume rm lakehouse-stack_uc-data

# Restart UC (starts with a fresh database)
./lakehouse start unity-catalog

# Re-register your catalogs and schemas
```

For production, use PostgreSQL instead of H2:

```properties
# In server.properties
hibernate.connection.driver_class=org.postgresql.Driver
hibernate.connection.url=jdbc:postgresql://localhost:5432/unity_catalog
hibernate.connection.username=postgres
hibernate.connection.password=your_password
hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
```

---

## 15. References

### Official Documentation

| Resource | URL |
|----------|-----|
| Unity Catalog Documentation | [docs.unitycatalog.io](https://docs.unitycatalog.io/) |
| Unity Catalog GitHub | [github.com/unitycatalog/unitycatalog](https://github.com/unitycatalog/unitycatalog) |
| UC API Reference | [docs.unitycatalog.io/api](https://docs.unitycatalog.io/api/) |
| LF AI & Data Foundation | [lfaidata.foundation](https://lfaidata.foundation/) |
| UC Docker Hub | [hub.docker.com/r/unitycatalog/unitycatalog](https://hub.docker.com/r/unitycatalog/unitycatalog) |

### Apache Iceberg

| Resource | URL |
|----------|-----|
| Iceberg REST Catalog Spec (OpenAPI) | [github.com/apache/iceberg/.../rest-catalog-open-api.yaml](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) |
| Iceberg REST Catalog Documentation | [iceberg.apache.org/concepts/catalog](https://iceberg.apache.org/concepts/catalog/) |
| Iceberg Spark Configuration | [iceberg.apache.org/docs/latest/spark-configuration](https://iceberg.apache.org/docs/latest/spark-configuration/) |
| PyIceberg Documentation | [py.iceberg.apache.org](https://py.iceberg.apache.org/) |

### Engine Integration Guides

| Engine | Documentation |
|--------|--------------|
| Spark + Iceberg | [iceberg.apache.org/docs/latest/spark-getting-started](https://iceberg.apache.org/docs/latest/spark-getting-started/) |
| DuckDB Iceberg Extension | [duckdb.org/docs/extensions/iceberg](https://duckdb.org/docs/extensions/iceberg.html) |
| Trino Iceberg Connector | [trino.io/docs/current/connector/iceberg](https://trino.io/docs/current/connector/iceberg.html) |
| Dremio Iceberg REST | [docs.dremio.com/.../iceberg-rest](https://docs.dremio.com/current/sonar/data-sources/iceberg-rest/) |
| Flink + Iceberg | [iceberg.apache.org/docs/latest/flink](https://iceberg.apache.org/docs/latest/flink/) |

### Related Tools

| Tool | URL | Relevance |
|------|-----|-----------|
| OpenLineage | [openlineage.io](https://openlineage.io/) | Data lineage (fills UC OSS gap) |
| Marquez | [marquezproject.ai](https://marquezproject.ai/) | Lineage server compatible with OpenLineage |
| DataHub | [datahubproject.io](https://datahubproject.io/) | Metadata platform (alternative governance approach) |
| Delta Sharing | [github.com/delta-io/delta-sharing](https://github.com/delta-io/delta-sharing) | Data sharing protocol (not in UC OSS) |
| MLflow | [mlflow.org](https://mlflow.org/) | ML platform with UC model registry integration |

### lakehouse-stack References

| File | Purpose |
|------|---------|
| `docker-compose-unity-catalog.yml` | UC Docker Compose configuration |
| `config/unity-catalog/server.properties.example` | UC server configuration template |
| `config/spark/spark-defaults-uc.conf.example` | Spark config for UC integration |
| `docs/guides/unity-catalog.md` | Quick-start guide for UC in lakehouse-stack |
| `scripts/demos/overarchitected/02_unity_catalog_setup.py` | Act 2 demo script |

### Blog Posts and Talks

| Title | URL | Date |
|-------|-----|------|
| "Open Sourcing Unity Catalog" (Databricks) | [databricks.com/blog/open-sourcing-unity-catalog](https://www.databricks.com/blog/open-sourcing-unity-catalog) | June 2024 |
| "Unity Catalog Joins LF AI & Data" | [lfaidata.foundation/blog/...](https://lfaidata.foundation/blog/2024/08/15/unity-catalog-joins-lf-ai-data-foundation/) | August 2024 |
| "Iceberg REST Catalog: A Universal Catalog Interface" (Tabular) | [tabular.io/blog/rest-catalog](https://tabular.io/blog/rest-catalog/) | 2023 |
| "Credential Vending in Open Table Formats" (Dremio) | [dremio.com/blog/credential-vending](https://www.dremio.com/blog/credential-vending-in-apache-iceberg/) | 2024 |

---

*This companion guide is part of the OverArchitected show series. For the complete show flow, see `scripts/demos/overarchitected/README.md`.*
