#!/usr/bin/env python3
"""
MLflow Pipeline Guardian Agent
======================================================

"We're lazy. Can AI manage the lakehouse for us?"

Option A: Pipeline Guardian — an agent that inspects Iceberg table health,
checks data quality, and recommends/triggers maintenance actions.

Architecture:
  MLflow Agent Server (FastAPI) → ResponsesAgent → Tools:
    - inspect_table: check row counts, snapshots, file counts
    - check_quality: null rates, freshness, duplicate detection
    - run_maintenance: trigger compaction, snapshot expiry
    - query_table: run arbitrary SQL for investigation

  All LLM calls routed through MLflow AI Gateway.
  Every action traced via MLflow Tracing (OpenTelemetry).
  Agent registered in Unity Catalog model registry.

Run:
    # Start MLflow tracking server first:
    docker compose -f docker-compose-mlflow.yml up -d

    # Run the agent:
    python scripts/demos/mlflow-agents/guardian.py

    # Or serve it:
    mlflow models serve -m runs:/<run_id>/guardian_agent -p 5001

Prerequisites:
    pip install mlflow>=3.1 openai pyspark
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
    # Uses Ollama (local OSS models) via OpenAI-compatible API. No API keys needed.
"""

import os
import json
import uuid
from datetime import datetime, timedelta

# MLflow imports
import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# PySpark thin client
from pyspark.sql import SparkSession


# ─── Configuration ──────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
SPARK_REMOTE = os.getenv("SPARK_REMOTE", None)  # sc://host:15002 for Connect
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen3-coder:30b")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")  # "openai" for Ollama-compatible API
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ─── Spark Connection ──────────────────────────────────────
def get_spark():
    """Get Spark session — prefer Connect if available."""
    if SPARK_REMOTE:
        return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("MLflow-Guardian-Agent") \
        .getOrCreate()


# ─── Tool Functions ─────────────────────────────────────────
def inspect_table(table_name: str) -> dict:
    """Inspect an Iceberg table: row count, snapshots, partition info."""
    spark = get_spark()
    result = {"table": table_name}

    try:
        count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
        result["row_count"] = count
    except Exception as e:
        result["error"] = f"Cannot read table: {e}"
        return result

    # Snapshot history
    try:
        snapshots = spark.sql(
            f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC"
        ).limit(10).collect()
        result["snapshot_count"] = len(snapshots)
        if snapshots:
            result["latest_snapshot"] = str(snapshots[0]["committed_at"])
            result["oldest_snapshot"] = str(snapshots[-1]["committed_at"])
    except Exception:
        result["snapshot_count"] = "unknown"

    # File metadata
    try:
        files = spark.sql(
            f"SELECT COUNT(*) as file_count, "
            f"SUM(file_size_in_bytes) as total_bytes "
            f"FROM {table_name}.files"
        ).collect()[0]
        result["file_count"] = files["file_count"]
        result["total_size_mb"] = round(files["total_bytes"] / (1024 * 1024), 2) if files["total_bytes"] else 0
    except Exception:
        result["file_count"] = "unknown"

    return result


def check_quality(table_name: str) -> dict:
    """Check data quality: null rates, freshness, basic stats."""
    spark = get_spark()
    result = {"table": table_name, "checks": []}

    try:
        df = spark.table(table_name)
        total = df.count()
        result["total_rows"] = total

        if total == 0:
            result["checks"].append({"check": "empty_table", "status": "FAIL", "detail": "Table has 0 rows"})
            return result

        # Null checks on key columns
        for col_name in df.columns[:10]:  # Check first 10 columns
            null_count = df.filter(f"`{col_name}` IS NULL").count()
            null_pct = round(100 * null_count / total, 2)
            status = "WARN" if null_pct > 5 else "PASS"
            if null_pct > 20:
                status = "FAIL"
            result["checks"].append({
                "check": f"null_rate_{col_name}",
                "status": status,
                "null_count": null_count,
                "null_pct": null_pct,
            })

        # Freshness check (look for timestamp columns)
        ts_cols = [c.name for c in df.schema if "timestamp" in c.dataType.simpleString().lower()
                   or "ts" in c.name.lower()]
        if ts_cols:
            ts_col = ts_cols[0]
            try:
                latest = spark.sql(
                    f"SELECT MAX(`{ts_col}`) FROM {table_name}"
                ).collect()[0][0]
                if latest:
                    freshness_hours = (datetime.now() - latest).total_seconds() / 3600
                    status = "PASS" if freshness_hours < 24 else "WARN"
                    if freshness_hours > 72:
                        status = "FAIL"
                    result["checks"].append({
                        "check": "data_freshness",
                        "status": status,
                        "latest_timestamp": str(latest),
                        "hours_since_update": round(freshness_hours, 1),
                    })
            except Exception:
                pass

        # Duplicate check on first column (likely ID)
        id_col = df.columns[0]
        distinct_count = df.select(id_col).distinct().count()
        dup_count = total - distinct_count
        if dup_count > 0:
            result["checks"].append({
                "check": f"duplicates_{id_col}",
                "status": "WARN",
                "duplicate_count": dup_count,
                "duplicate_pct": round(100 * dup_count / total, 2),
            })

    except Exception as e:
        result["error"] = str(e)

    return result


def run_maintenance(table_name: str, action: str) -> dict:
    """Run Iceberg maintenance: compact, expire_snapshots, remove_orphans."""
    spark = get_spark()
    result = {"table": table_name, "action": action}

    try:
        if action == "compact" or action == "rewrite_data_files":
            spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{table_name}')")
            result["status"] = "completed"
            result["detail"] = "Data files rewritten (compacted)"

        elif action == "expire_snapshots":
            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            spark.sql(
                f"CALL iceberg.system.expire_snapshots("
                f"table => '{table_name}', "
                f"older_than => TIMESTAMP '{cutoff}', "
                f"retain_last => 5)"
            )
            result["status"] = "completed"
            result["detail"] = f"Snapshots older than {cutoff} expired (keeping last 5)"

        elif action == "remove_orphans":
            cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            spark.sql(
                f"CALL iceberg.system.remove_orphan_files("
                f"table => '{table_name}', "
                f"older_than => TIMESTAMP '{cutoff}')"
            )
            result["status"] = "completed"
            result["detail"] = f"Orphan files older than {cutoff} removed"

        else:
            result["status"] = "error"
            result["detail"] = f"Unknown action: {action}. Use: compact, expire_snapshots, remove_orphans"

    except Exception as e:
        result["status"] = "error"
        result["detail"] = str(e)

    return result


def query_table(sql: str) -> dict:
    """Run arbitrary SQL against the lakehouse. Returns up to 20 rows."""
    spark = get_spark()
    result = {"sql": sql}

    try:
        df = spark.sql(sql)
        rows = df.limit(20).collect()
        result["columns"] = df.columns
        result["row_count"] = len(rows)
        result["data"] = [row.asDict() for row in rows]
    except Exception as e:
        result["error"] = str(e)

    return result


# ─── Tool Registry ──────────────────────────────────────────
TOOLS = {
    "inspect_table": {
        "fn": inspect_table,
        "description": "Inspect an Iceberg table: row count, snapshots, file counts, total size",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name (e.g., iceberg.bronze.orders)"}
            },
            "required": ["table_name"]
        }
    },
    "check_quality": {
        "fn": check_quality,
        "description": "Check data quality: null rates, freshness, duplicates",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name"}
            },
            "required": ["table_name"]
        }
    },
    "run_maintenance": {
        "fn": run_maintenance,
        "description": "Run Iceberg maintenance: compact (rewrite_data_files), expire_snapshots, remove_orphans",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "Fully qualified table name"},
                "action": {"type": "string", "enum": ["compact", "expire_snapshots", "remove_orphans"]}
            },
            "required": ["table_name", "action"]
        }
    },
    "query_table": {
        "fn": query_table,
        "description": "Run SQL against the lakehouse. Returns up to 20 rows. Use for investigation.",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL query to execute"}
            },
            "required": ["sql"]
        }
    },
}

TOOL_SCHEMAS = [
    {"type": "function", "function": {"name": name, "description": info["description"], "parameters": info["parameters"]}}
    for name, info in TOOLS.items()
]

SYSTEM_PROMPT = """You are the Pipeline Guardian — an AI agent that monitors and maintains
a data lakehouse built on Apache Spark + Iceberg + PostgreSQL + SeaweedFS.

Your responsibilities:
1. Inspect table health (row counts, snapshots, file counts)
2. Check data quality (null rates, freshness, duplicates)
3. Recommend and execute maintenance (compaction, snapshot expiry, orphan cleanup)
4. Answer questions about the data by running SQL queries

Available tables:
- iceberg.bronze.orders (raw order events)
- iceberg.bronze.dim_locations (delivery locations)
- iceberg.bronze.dim_brands (restaurant brands)
- iceberg.bronze.dim_items (menu items)
- iceberg.bronze.dim_categories (item categories)
- iceberg.silver.orders_enriched (if pipeline has run)
- iceberg.gold.hourly_metrics (if pipeline has run)

When asked to check health, inspect all bronze tables and report findings.
When you find issues, recommend specific maintenance actions.
Be concise and technical. This is for a live demo."""


# ─── Agent Implementation ──────────────────────────────────
class PipelineGuardianAgent(ChatAgent):
    """MLflow ChatAgent that guards the lakehouse pipeline."""

    def __init__(self):
        self._client = None

    @property
    def client(self):
        if self._client is None:
            import openai
            self._client = openai.OpenAI(base_url=OLLAMA_BASE_URL, api_key="ollama")
        return self._client

    def _call_llm(self, messages, tools=None):
        """Call the LLM via OpenAI-compatible API (Ollama)."""
        kwargs = {
            "model": LLM_MODEL,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
            "max_tokens": 4096,
        }
        if tools:
            kwargs["tools"] = tools
        return self.client.chat.completions.create(**kwargs)

    def _extract_response(self, response):
        """Extract text and tool calls from OpenAI-compatible response."""
        msg = response.choices[0].message
        text = msg.content or ""
        tool_calls = []
        if msg.tool_calls:
            for tc in msg.tool_calls:
                tool_calls.append({
                    "id": tc.id,
                    "name": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                })
        return text, tool_calls, response.choices[0].finish_reason

    @mlflow.trace
    def predict(self, messages, context=None, custom_inputs=None):
        """Run the agent with tool-calling loop."""
        # Convert ChatAgentMessage to dicts
        conv = [{"role": m.role, "content": m.content} for m in messages]

        max_iterations = 10
        all_text = []

        for _ in range(max_iterations):
            response = self._call_llm(conv, tools=TOOL_SCHEMAS)
            text, tool_calls, stop_reason = self._extract_response(response)

            if text:
                all_text.append(text)

            if not tool_calls:
                break

            # Execute tool calls (OpenAI-compatible format)
            conv.append({
                "role": "assistant",
                "content": text,
                "tool_calls": [
                    {"id": tc["id"], "type": "function",
                     "function": {"name": tc["name"], "arguments": json.dumps(tc["arguments"])}}
                    for tc in tool_calls
                ],
            })
            for tc in tool_calls:
                tool_fn = TOOLS[tc["name"]]["fn"]
                with mlflow.start_span(name=f"tool:{tc['name']}") as span:
                    span.set_inputs(tc["arguments"])
                    result = tool_fn(**tc["arguments"])
                    span.set_outputs(result)

                conv.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(result, default=str),
                })

        final_text = "\n".join(all_text) if all_text else "No response generated."
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=final_text, id=str(uuid.uuid4()))]
        )


# ─── Demo Runner ────────────────────────────────────────────
def run_demo():
    """Run the Pipeline Guardian agent interactively."""
    section("MLflow Pipeline Guardian Agent")

    # Set up MLflow tracking (optional — works without tracking server)
    tracking_available = False
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("overarchitected-guardian")
        tracking_available = True
    except Exception:
        print("  MLflow tracking server not available — running without tracking.")

    if tracking_available:
        try:
            mlflow.openai.autolog()
        except Exception:
            pass

    agent = PipelineGuardianAgent()

    # Demo queries
    demo_queries = [
        "Check the health of all bronze tables. Give me a summary.",
        "What's the data quality like in iceberg.bronze.orders? Any issues?",
        "Run compaction on iceberg.bronze.orders and expire old snapshots.",
    ]

    print(f"\n  LLM Provider: {LLM_PROVIDER}")
    print(f"  LLM Model: {LLM_MODEL}")
    print(f"  MLflow Tracking: {MLFLOW_TRACKING_URI}")
    print(f"  Spark: {SPARK_REMOTE or SPARK_MASTER}")

    for i, query in enumerate(demo_queries, 1):
        print(f"\n{'═' * 60}")
        print(f"  Demo Query {i}: {query}")
        print(f"{'═' * 60}")

        try:
            mlflow.end_run()
            run_ctx = mlflow.start_run(run_name=f"guardian_demo_{i}") if tracking_available else None
            if run_ctx:
                run_ctx.__enter__()
            response = agent.predict(
                messages=[ChatAgentMessage(role="user", content=query)]
            )
            print(f"\n  Agent Response:")
            print(f"  {'-' * 50}")
            for msg in response.messages:
                print(f"  {msg.content}")
            if run_ctx:
                run_ctx.__exit__(None, None, None)
        except Exception as e:
            print(f"  Error: {e}")
            import traceback; traceback.print_exc()

    # Log the agent model
    if tracking_available:
        section("Logging Agent to MLflow")
        try:
            with mlflow.start_run(run_name="guardian_agent_registration"):
                mlflow.pyfunc.log_model(
                    artifact_path="guardian_agent",
                    python_model=agent,
                    registered_model_name="pipeline-guardian",
                )
                print("  Agent logged to MLflow model registry as 'pipeline-guardian'")
        except Exception as e:
            print(f"  Could not log model: {e}")


def main():
    print("\n" + "=" * 60)
    print("  PIPELINE GUARDIAN AGENT")
    print("  AI manages the lakehouse. We watch.")
    print("=" * 60)

    run_demo()

    print("\n" + "=" * 60)
    print("  Done.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
