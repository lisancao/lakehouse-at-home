# Lakehouse-in-a-box Stack

A fully open-source, self-hostable data lakehouse for local development and testing of OSS data workflows.

## Stack

- **Apache Spark 4.0.1 / 4.1.0** - Distributed compute engine (supports both versions simultaneously)
- **Apache Iceberg 1.10.0** - Table format with ACID transactions
- **Apache Kafka 3.6.1** - Event streaming platform
- **PostgreSQL 16** - Iceberg catalog metadata
- **SeaweedFS** - S3-compatible object storage

## Architecture

```
Spark 4.1 (Master/Workers)
    ↓
Iceberg 1.10 (Table Format)
    ├── Metadata → PostgreSQL
    └── Data → SeaweedFS (S3)
    ↓
Kafka 3.6.1 (Streaming)
```

## Prerequisites

- Docker & Docker Compose v2
- PostgreSQL 16 (running natively on host)
- SeaweedFS (running natively on host)
- Python 3.10+ with Poetry
- 20GB disk space for JARs and data

## Quick Start

### 1. Clone and Configure

```bash
git clone https://github.com/lisancao/lakehouse-at-home.git
cd lakehouse-at-home

# Copy environment template
cp .env.example .env
nano .env  # Edit with your credentials

# Copy Spark config template
cp config/spark/spark-defaults.conf.example config/spark/spark-defaults.conf
nano config/spark/spark-defaults.conf  # Edit with your credentials
```

### 2. Download JARs

```bash
./scripts/download-jars.sh
```

This downloads ~860MB of dependencies (exact versions required for compatibility).

### 3. Install Python Dependencies

```bash
poetry install
```

### 4. Start Services

```bash
# Using the CLI
./lakehouse start all

# Or manually with Docker Compose
docker compose -f docker-compose-spark41.yml up -d
docker compose -f docker-compose-kafka.yml up -d
```

### 5. Verify Setup

```bash
./lakehouse test
./scripts/run-spark-test.sh
```

## Usage

### CLI Commands

```bash
./lakehouse status                              # Check all services status
./lakehouse start [spark|kafka|all]             # Start services
./lakehouse stop [spark|kafka|all]              # Stop services
./lakehouse restart [spark|kafka|all]           # Restart services
./lakehouse logs [spark-master|spark-worker|kafka|zookeeper]  # View logs
./lakehouse test                                # Run connectivity tests
./lakehouse producer                            # Start Kafka event producer
./lakehouse consumer                            # Start Spark streaming consumer

# Version flag (default: 4.1)
./lakehouse start spark --version 4.0           # Start Spark 4.0
./lakehouse start spark --version 4.1           # Start Spark 4.1
./lakehouse consumer --version 4.0              # Run consumer on Spark 4.0
```

### Example Scripts

| Script | Description |
|--------|-------------|
| `scripts/01-basics.py` | Basic Spark table operations |
| `scripts/02-transformations.py` | Narrow & wide transformations |
| `scripts/03-streaming-basic.py` | Streaming with rate source |
| `scripts/04-kafka-streaming.py` | Kafka streaming with windowed aggregations |
| `scripts/kafka-producer.py` | Synthetic event generator |
| `scripts/test-kafka.py` | Simple Kafka consumer (used by `./lakehouse consumer`) |
| `scripts/test-iceberg.py` | Iceberg catalog connectivity test |

### Kafka Streaming Example

```bash
# Terminal 1: Start producer
./lakehouse producer

# Terminal 2: Simple consumer (via CLI)
./lakehouse consumer

# Alternative: Advanced windowed aggregations
docker exec spark-master-41 /opt/spark/bin/spark-submit \
  --master local[2] /scripts/04-kafka-streaming.py
```

## Configuration

### Environment Variables (.env)

```
POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT
S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
ICEBERG_CATALOG_URI, ICEBERG_WAREHOUSE
```

### Spark Configuration

Edit `config/spark/spark-defaults.conf` for Spark tuning and credentials.

## Multi-Version Spark Testing

Run Spark 4.0 and 4.1 simultaneously for A/B testing and migration validation.

### Port Assignments

| Version | Compose File | Master Port | UI Port | Worker UI |
|---------|--------------|-------------|---------|-----------|
| 4.0.1 | `docker-compose.yml` | 7077 | 8080 | 8081 |
| 4.1.0 | `docker-compose-spark41.yml` | 7078 | 8082 | 8083 |

### Running Both Versions

```bash
# Start both Spark versions
./lakehouse start spark --version 4.0
./lakehouse start spark --version 4.1

# Check status (shows both)
./lakehouse status

# Submit jobs to specific version
spark-submit --master spark://localhost:7077 script.py  # Spark 4.0
spark-submit --master spark://localhost:7078 script.py  # Spark 4.1

# Stop specific version
./lakehouse stop spark --version 4.0
```

## Table Naming Convention (Medallion)

- `iceberg.bronze.*` - Raw data
- `iceberg.silver.*` - Cleaned/transformed
- `iceberg.gold.*` - Business-ready

## Critical Version Locks

This stack requires **exact** versions for compatibility:

| Component | Version | Notes |
|-----------|---------|-------|
| Spark | 4.0.1 | Scala 2.13, Java 17 |
| Spark | 4.1.0 | Scala 2.13, Java 21 |
| Iceberg | 1.10.0 | For Spark 4.x |
| Hadoop AWS | 3.4.1 | - |
| AWS SDK v2 | 2.24.6 | **Exact match required** |
| Kafka | 3.6.1 | - |
| commons-pool2 | 2.12.0 | For Kafka streaming |

## Troubleshooting

### Connection Refused

Ensure PostgreSQL and SeaweedFS are running on the host:
```bash
systemctl status postgresql
systemctl status seaweedfs-master
```

### Kafka Not Connecting

Verify Zookeeper is healthy before Kafka:
```bash
docker logs zookeeper
docker logs kafka
```

### Spark UI

| Version | Master UI | Worker UI |
|---------|-----------|-----------|
| 4.0 | http://localhost:8080 | http://localhost:8081 |
| 4.1 | http://localhost:8082 | http://localhost:8083 |

- Application UI: http://localhost:4040 (active job)

## License

MIT
