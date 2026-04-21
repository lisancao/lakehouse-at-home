# MLflow Agent Demos

Three reference agents that use MLflow's AI Gateway, Tracing, and Model
Registry to manage the lakehouse. Each is a standalone FastAPI service
that wraps a `ResponsesAgent` with a tool set scoped to lakehouse
operations.

## Prerequisites

```bash
pip install mlflow>=3.1 openai pyspark
./lakehouse start all            # Spark, Kafka, UC
docker compose -f docker-compose-mlflow.yml up -d  # Tracking + Gateway
./lakehouse testdata generate --days 7
./lakehouse testdata load
```

The agents default to Ollama (local OSS) via the Gateway's OpenAI-compatible
API. To switch to Anthropic, set `MLFLOW_GATEWAY_MODEL=anthropic/claude-opus-4-7`
(or whichever route you configured in `config/mlflow/gateway-config.yml`).

## Agents

| Agent | Role |
|-------|------|
| `guardian.py` | Pipeline Guardian — inspects Iceberg table health, checks data quality, triggers maintenance (compaction, snapshot expiry) |
| `analyst.py` | Analyst — answers business questions over the gold layer using SQL |
| `autopilot.py` | Autopilot — orchestrates multiple tools to diagnose and remediate pipeline issues end-to-end |

## Running

```bash
python scripts/demos/mlflow-agents/guardian.py
python scripts/demos/mlflow-agents/analyst.py
python scripts/demos/mlflow-agents/autopilot.py
```

Or serve an agent as a registered MLflow model:

```bash
mlflow models serve -m runs:/<run_id>/guardian_agent -p 5001
```

All LLM calls are routed through the MLflow AI Gateway and traced via MLflow
Tracing (OpenTelemetry). Agent artifacts are registered in the Unity Catalog
model registry.
