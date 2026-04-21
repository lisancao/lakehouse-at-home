# Troubleshooting

Common issues and solutions for the lakehouse stack.

## Quick Diagnostics

```bash
# Check all services
./lakehouse test

# Machine-readable status
./lakehouse status --json

# View service logs
./lakehouse logs spark-master
./lakehouse logs kafka
```

## PostgreSQL Issues

### Connection Refused

**Symptom**: `psql: could not connect to server: Connection refused`

**Solutions**:

```bash
# Check if PostgreSQL is running
systemctl status postgresql    # Linux
brew services list             # macOS

# Start PostgreSQL
sudo systemctl start postgresql    # Linux
brew services start postgresql@16  # macOS
```

### Authentication Failed

**Symptom**: `FATAL: password authentication failed for user`

**Solutions**:

```bash
# Verify credentials in .env
cat .env | grep POSTGRES

# Test connection manually
PGPASSWORD=yourpassword psql -h localhost -U lakehouse -d iceberg_catalog

# Check pg_hba.conf (Linux)
sudo nano /etc/postgresql/16/main/pg_hba.conf
# Ensure: local all all md5
# Then: sudo systemctl restart postgresql
```

### Database Does Not Exist

**Symptom**: `FATAL: database "iceberg_catalog" does not exist`

**Solution**:

```bash
# Create the database
createdb -O lakehouse iceberg_catalog

# Or with sudo on Linux
sudo -u postgres createdb -O lakehouse iceberg_catalog
```

## SeaweedFS Issues

### Not Responding

**Symptom**: `curl: (7) Failed to connect to localhost port 8333`

**Solutions**:

```bash
# Check if running
pgrep -a weed

# Start SeaweedFS
weed server -s3 -dir=/tmp/seaweedfs &

# Or with Docker
docker run -d --name seaweedfs -p 8333:8333 -p 9333:9333 \
  chrislusf/seaweedfs server -s3
```

### Permission Denied

**Symptom**: S3 access denied errors

**Solutions**:

```bash
# SeaweedFS doesn't enforce credentials by default
# Check that access keys in .env match spark-defaults.conf

# Verify S3 connectivity
curl http://localhost:8333
```

## Docker Issues

### Permission Denied

**Symptom**: `permission denied while trying to connect to the Docker daemon`

**Solution**:

```bash
# Add user to docker group
sudo usermod -aG docker $USER

# Apply without logout
newgrp docker

# Verify
docker ps
```

### Container Not Starting

**Symptom**: Container exits immediately

**Solutions**:

```bash
# Check logs
docker logs spark-master-41

# Check for port conflicts
sudo lsof -i :7078
sudo lsof -i :8082

# Remove orphan containers
docker compose -f docker-compose-spark41.yml down --remove-orphans
docker compose -f docker-compose-spark41.yml up -d
```

### Out of Disk Space

**Symptom**: `no space left on device`

**Solutions**:

```bash
# Check disk usage
df -h
du -sh jars/ data/

# Docker cleanup
docker system prune -f
docker volume prune -f

# Remove generated test data
./lakehouse testdata clean
```

## Spark Issues

### `./data/` Permission Denied (Spark can't write, or testdata can't)

**Symptom:** Spark driver/executor errors trying to write to `/data/spark`
or `./data`, or `./lakehouse testdata generate` fails with `Permission
denied` after a container has already written there.

**Why it happens:** The official `apache/spark` images run as UID `185`
(the `spark` user), but host-side commands like `testdata generate` run
as your host UID (typically `1000` on Linux). If either side takes
exclusive ownership of `./data/`, the other side can no longer write.

**Fix — share the group:**

```bash
# User 185 (spark) owns the files, but the host group keeps write access.
sudo chown -R 185 data/
sudo chmod -R g+w data/
sudo chmod g+s data/   # new files inherit the directory's group (sgid)
```

The `g+s` sgid bit is the key — it ensures any file Spark creates
inside `./data/` adopts the directory's group, so the host user retains
write access via group membership.

**Alternative — run testdata inside the container:**

```bash
docker exec spark-master-41 python -m scripts.testdata.generator --days 7
```

No permission dance needed: the process runs as UID 185 directly, same
as Spark itself.

### JARs Not Found

**Symptom**: `ClassNotFoundException` or `NoClassDefFoundError`

**Solutions**:

```bash
# Download JARs
./scripts/tools/download-jars.sh

# Verify JARs exist
ls -la jars/

# Expected files:
# - iceberg-spark-runtime-4.0_2.13-1.10.0.jar
# - hadoop-aws-3.4.1.jar
# - aws-java-sdk-bundle-1.12.780.jar
# - bundle-2.24.6.jar
# - postgresql-42.7.4.jar
```

### Wrong Java Version

**Symptom**: `UnsupportedClassVersionError`

**Solutions**:

```bash
# Check Java version
java -version

# Spark 4.0 needs Java 17
# Spark 4.1 needs Java 21

# macOS - switch Java
export JAVA_HOME=$(/usr/libexec/java_home -v 21)

# Linux - use update-alternatives
sudo update-alternatives --config java
```

### Iceberg Catalog Connection Failed

**Symptom**: `Unable to connect to catalog`

**Solutions**:

```bash
# Verify PostgreSQL is running
./lakehouse test

# Check JDBC URL in spark-defaults.conf
grep "jdbc.uri" config/spark/spark-defaults.conf

# Test direct connection
docker exec spark-master-41 /opt/spark/bin/spark-sql \
  -e "SHOW NAMESPACES IN iceberg"
```

### S3 Connection Failed

**Symptom**: `Unable to find valid credentials`

**Solutions**:

```bash
# Verify SeaweedFS is running
curl http://localhost:8333

# Check spark-defaults.conf
grep "fs.s3a" config/spark/spark-defaults.conf

# Ensure path.style.access is true for SeaweedFS
# spark.hadoop.fs.s3a.path.style.access=true
```

## Kafka Issues

### Broker Not Available

**Symptom**: `Broker may not be available`

**Solutions**:

```bash
# Check Zookeeper first (Kafka depends on it)
docker logs zookeeper

# Then check Kafka
docker logs kafka

# Restart both
./lakehouse stop kafka
./lakehouse start kafka
```

### Topic Not Found

**Symptom**: `Topic not found`

**Solutions**:

```bash
# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Create topic
docker exec kafka kafka-topics --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1
```

## Poetry/Python Issues

### Poetry Not Found

**Symptom**: `command not found: poetry`

**Solution**:

```bash
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Add to PATH (add to ~/.bashrc or ~/.zshrc)
export PATH="$HOME/.local/bin:$PATH"
```

### Dependency Conflicts

**Symptom**: `SolverProblemError`

**Solutions**:

```bash
# Clear cache
poetry cache clear . --all

# Regenerate lock file
poetry lock --no-update

# Install
poetry install
```

### Wrong Python Version

**Symptom**: `requires python >=3.10`

**Solutions**:

```bash
# Check Python version
python3 --version

# Install Python 3.11
# Ubuntu
sudo apt install python3.11

# macOS
brew install python@3.11

# Set Poetry to use correct version
poetry env use python3.11
```

## Port Conflicts

### Port Already in Use

**Symptom**: `address already in use`

**Solutions**:

```bash
# Find process using port
sudo lsof -i :8082

# Kill process
kill -9 <PID>

# Or use different ports (edit docker-compose files)
```

### Common Port Assignments

| Port | Service |
|------|---------|
| 2181 | Zookeeper |
| 5432 | PostgreSQL |
| 7077 | Spark 4.0 Master |
| 7078 | Spark 4.1 Master |
| 8080 | Spark 4.0 UI |
| 8082 | Spark 4.1 UI |
| 8333 | SeaweedFS |
| 9092 | Kafka |

## Getting Help

If these solutions don't work:

1. Check logs: `./lakehouse logs <service>`
2. Run diagnostics: `./lakehouse setup`
3. Open an issue: [GitHub Issues](https://github.com/lisancao/lakehouse-at-home/issues)

Include in your issue:
- Output of `./lakehouse status --json`
- Relevant log excerpts
- Your OS and Docker version
