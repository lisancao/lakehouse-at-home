#!/usr/bin/env bash
# MLflow entrypoint that ensures its Postgres role and database exist,
# then starts the tracking server. Safe to re-run; all DDL is IF NOT EXISTS.
set -euo pipefail

PG_HOST="${POSTGRES_HOST:-localhost}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_SUPERUSER="${POSTGRES_USER:-postgres}"
PG_SUPERPASS="${POSTGRES_PASSWORD:-}"

MLFLOW_PG_USER="${MLFLOW_PG_USER:-mlflow}"
MLFLOW_PG_PASS="${MLFLOW_PG_PASS:-mlflow_password}"
MLFLOW_PG_DB="${MLFLOW_PG_DB:-mlflow}"

echo "[mlflow-entrypoint] waiting for postgres at ${PG_HOST}:${PG_PORT}..."
for _ in $(seq 1 30); do
    if PGPASSWORD="${PG_SUPERPASS}" psql \
         -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d postgres \
         -c 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Fast path: if mlflow can already connect with its own credentials, skip
# provisioning entirely. This covers the common case where a user has set
# up the role and db once (manually or via ./lakehouse setup-mlflow).
if PGPASSWORD="${MLFLOW_PG_PASS}" psql \
     -h "${PG_HOST}" -p "${PG_PORT}" -U "${MLFLOW_PG_USER}" -d "${MLFLOW_PG_DB}" \
     -c 'SELECT 1' >/dev/null 2>&1; then
    echo "[mlflow-entrypoint] role '${MLFLOW_PG_USER}' and db '${MLFLOW_PG_DB}' already present"
else
    echo "[mlflow-entrypoint] attempting to provision role '${MLFLOW_PG_USER}' and db '${MLFLOW_PG_DB}'..."
    prov_ok=1
    PGPASSWORD="${PG_SUPERPASS}" psql \
        -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d postgres \
        -v ON_ERROR_STOP=1 <<SQL || prov_ok=0
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '${MLFLOW_PG_USER}') THEN
        CREATE ROLE ${MLFLOW_PG_USER} WITH LOGIN PASSWORD '${MLFLOW_PG_PASS}';
    END IF;
END
\$\$;
SQL

    if [[ "${prov_ok}" == "1" ]] && ! PGPASSWORD="${PG_SUPERPASS}" psql \
           -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d postgres \
           -tAc "SELECT 1 FROM pg_database WHERE datname='${MLFLOW_PG_DB}'" 2>/dev/null | grep -q 1; then
        PGPASSWORD="${PG_SUPERPASS}" psql \
            -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_SUPERUSER}" -d postgres \
            -v ON_ERROR_STOP=1 \
            -c "CREATE DATABASE ${MLFLOW_PG_DB} OWNER ${MLFLOW_PG_USER};" || prov_ok=0
    fi

    if [[ "${prov_ok}" == "0" ]]; then
        cat >&2 <<EOF
[mlflow-entrypoint] unable to auto-provision MLflow role/db.
POSTGRES_USER='${PG_SUPERUSER}' lacks CREATEROLE/CREATEDB, or the db is
unreachable. Provision once from a privileged session and restart:

    psql -U <superuser> -d postgres <<'SQL'
    CREATE ROLE ${MLFLOW_PG_USER} WITH LOGIN PASSWORD '<password>';
    CREATE DATABASE ${MLFLOW_PG_DB} OWNER ${MLFLOW_PG_USER};
    SQL

EOF
        exit 1
    fi
fi

echo "[mlflow-entrypoint] starting mlflow server"
exec mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri "postgresql://${MLFLOW_PG_USER}:${MLFLOW_PG_PASS}@${PG_HOST}:${PG_PORT}/${MLFLOW_PG_DB}" \
    --default-artifact-root "${MLFLOW_ARTIFACTS_DESTINATION:-s3://lakehouse/mlflow-artifacts}"
