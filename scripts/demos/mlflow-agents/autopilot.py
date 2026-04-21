#!/usr/bin/env python3
"""
MLflow Pipeline Autopilot Agent
========================================================

Option C (ambitious): An agent that monitors streaming pipeline health,
detects anomalies, and takes corrective action.

Architecture:
  Monitoring loop → Agent checks metrics → Decides actions → Executes
  - Throughput monitoring (events/sec)
  - Latency tracking (processing delay)
  - Data quality anomalies (null spikes, schema drift)
  - Auto-maintenance (compaction when file count is high)
  - Alerting (when metrics exceed thresholds)

This is the most complex option. It ties together:
  Spark + Iceberg + Kafka + Airflow + MLflow

Run:
    python scripts/demos/mlflow-agents/autopilot.py

    # Or in continuous monitoring mode:
    python scripts/demos/mlflow-agents/autopilot.py --monitor

Prerequisites:
    pip install mlflow>=3.1 openai pyspark
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

import os
import sys
import json
import time
import uuid
from datetime import datetime, timedelta

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentMessage,
    ChatAgentResponse,
)

from pyspark.sql import SparkSession


# ─── Configuration ──────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
SPARK_REMOTE = os.getenv("SPARK_REMOTE", None)
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen3-coder:30b")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")

# Thresholds for anomaly detection
THRESHOLDS = {
    "max_null_rate_pct": 10.0,
    "max_stale_hours": 24,
    "max_file_count": 500,
    "min_row_count": 100,
    "max_snapshot_count": 20,
}


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def get_spark():
    if SPARK_REMOTE:
        return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("MLflow-Autopilot-Agent") \
        .getOrCreate()


# ─── Monitoring Tools ───────────────────────────────────────
def collect_pipeline_metrics() -> dict:
    """Collect comprehensive metrics across all lakehouse tables."""
    spark = get_spark()
    metrics = {"timestamp": datetime.now().isoformat(), "tables": {}, "anomalies": []}

    tables = [
        "iceberg.bronze.orders",
        "iceberg.bronze.dim_locations",
        "iceberg.bronze.dim_brands",
        "iceberg.bronze.dim_items",
        "iceberg.bronze.dim_categories",
    ]

    for table in tables:
        table_metrics = {"name": table}
        try:
            # Row count
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            table_metrics["row_count"] = count

            if count < THRESHOLDS["min_row_count"]:
                metrics["anomalies"].append({
                    "type": "low_row_count",
                    "table": table,
                    "value": count,
                    "threshold": THRESHOLDS["min_row_count"],
                    "severity": "WARNING",
                })

            # Null rates on key columns
            df = spark.table(table)
            for col in df.columns[:5]:
                null_count = df.filter(f"`{col}` IS NULL").count()
                null_pct = round(100 * null_count / max(count, 1), 2)
                table_metrics[f"null_pct_{col}"] = null_pct

                if null_pct > THRESHOLDS["max_null_rate_pct"]:
                    metrics["anomalies"].append({
                        "type": "high_null_rate",
                        "table": table,
                        "column": col,
                        "value": null_pct,
                        "threshold": THRESHOLDS["max_null_rate_pct"],
                        "severity": "WARNING",
                    })

            # File count (Iceberg metadata)
            try:
                file_count = spark.sql(
                    f"SELECT COUNT(*) FROM {table}.files"
                ).collect()[0][0]
                table_metrics["file_count"] = file_count

                if file_count > THRESHOLDS["max_file_count"]:
                    metrics["anomalies"].append({
                        "type": "high_file_count",
                        "table": table,
                        "value": file_count,
                        "threshold": THRESHOLDS["max_file_count"],
                        "severity": "ACTION_NEEDED",
                        "recommended_action": "compact",
                    })
            except Exception:
                table_metrics["file_count"] = "unknown"

            # Snapshot count
            try:
                snap_count = spark.sql(
                    f"SELECT COUNT(*) FROM {table}.snapshots"
                ).collect()[0][0]
                table_metrics["snapshot_count"] = snap_count

                if snap_count > THRESHOLDS["max_snapshot_count"]:
                    metrics["anomalies"].append({
                        "type": "high_snapshot_count",
                        "table": table,
                        "value": snap_count,
                        "threshold": THRESHOLDS["max_snapshot_count"],
                        "severity": "ACTION_NEEDED",
                        "recommended_action": "expire_snapshots",
                    })
            except Exception:
                table_metrics["snapshot_count"] = "unknown"

        except Exception as e:
            table_metrics["error"] = str(e)

        metrics["tables"][table] = table_metrics

    return metrics


def execute_maintenance(table_name: str, action: str) -> dict:
    """Execute maintenance action on a table."""
    spark = get_spark()
    result = {"table": table_name, "action": action, "timestamp": datetime.now().isoformat()}

    try:
        if action == "compact":
            spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{table_name}')")
            result["status"] = "success"
        elif action == "expire_snapshots":
            cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            spark.sql(
                f"CALL iceberg.system.expire_snapshots("
                f"table => '{table_name}', older_than => TIMESTAMP '{cutoff}', retain_last => 5)"
            )
            result["status"] = "success"
        elif action == "remove_orphans":
            cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            spark.sql(
                f"CALL iceberg.system.remove_orphan_files("
                f"table => '{table_name}', older_than => TIMESTAMP '{cutoff}')"
            )
            result["status"] = "success"
        else:
            result["status"] = "error"
            result["detail"] = f"Unknown action: {action}"
    except Exception as e:
        result["status"] = "error"
        result["detail"] = str(e)

    return result


def check_streaming_health() -> dict:
    """Check Kafka / streaming pipeline health."""
    result = {"timestamp": datetime.now().isoformat(), "streaming": {}}

    # Check Kafka connectivity
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        kafka_result = sock.connect_ex(("localhost", 9092))
        sock.close()
        result["streaming"]["kafka_reachable"] = kafka_result == 0
    except Exception:
        result["streaming"]["kafka_reachable"] = False

    # Check Spark streaming queries (if any active)
    try:
        spark = get_spark()
        active_queries = spark.streams.active
        result["streaming"]["active_queries"] = len(active_queries)
        for q in active_queries:
            result["streaming"][q.name or q.id] = {
                "status": q.status,
                "recent_progress": str(q.recentProgress[-1]) if q.recentProgress else "none",
            }
    except Exception:
        result["streaming"]["active_queries"] = "unknown"

    return result


# ─── Tool Registry ──────────────────────────────────────────
TOOLS = {
    "collect_metrics": {
        "fn": collect_pipeline_metrics,
        "description": "Collect comprehensive health metrics across all lakehouse tables: row counts, null rates, file counts, snapshot counts, anomalies",
    },
    "execute_maintenance": {
        "fn": execute_maintenance,
        "description": "Execute maintenance on a table: compact, expire_snapshots, remove_orphans",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "action": {"type": "string", "enum": ["compact", "expire_snapshots", "remove_orphans"]},
            },
            "required": ["table_name", "action"],
        },
    },
    "check_streaming": {
        "fn": check_streaming_health,
        "description": "Check streaming pipeline health: Kafka connectivity, active Spark streaming queries",
    },
}

TOOL_SCHEMAS = []
for name, info in TOOLS.items():
    schema = {
        "type": "function",
        "function": {
            "name": name,
            "description": info["description"],
            "parameters": info.get("parameters", {"type": "object", "properties": {}}),
        },
    }
    TOOL_SCHEMAS.append(schema)

SYSTEM_PROMPT = """You are the Pipeline Autopilot — an autonomous agent that monitors and
maintains a data lakehouse. You are responsible for:

1. Collecting pipeline metrics (table health, data quality, file counts)
2. Detecting anomalies (high null rates, stale data, too many small files)
3. Taking corrective action (compaction, snapshot expiry, orphan cleanup)
4. Checking streaming health (Kafka, active queries)

You run in a monitoring loop. Each cycle:
  1. Collect metrics from all tables
  2. Analyze for anomalies
  3. If anomalies found: decide and execute maintenance actions
  4. Report findings

Be proactive. If you see high file counts, compact. If you see too many snapshots, expire.
If streaming is down, report it. Be concise and action-oriented."""


# ─── Agent ──────────────────────────────────────────────────
class PipelineAutopilotAgent(ChatAgent):
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
            "model": LLM_MODEL, "max_tokens": 4096,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages,
        }
        if tools:
            kwargs["tools"] = tools
        return self.client.chat.completions.create(**kwargs)

    def _extract_response(self, response):
        """Extract text and tool calls from OpenAI-compatible response."""
        msg = response.choices[0].message
        tool_calls = []
        if msg.tool_calls:
            for tc in msg.tool_calls:
                tool_calls.append({"id": tc.id, "name": tc.function.name,
                                   "arguments": json.loads(tc.function.arguments)})
        return msg.content or "", tool_calls

    @mlflow.trace
    def predict(self, messages, context=None, custom_inputs=None):
        conv = [{"role": m.role, "content": m.content} for m in messages]
        all_text = []

        for _ in range(12):  # More iterations for autonomous actions
            response = self._call_llm(conv, tools=TOOL_SCHEMAS)
            text, tool_calls = self._extract_response(response)

            if text:
                all_text.append(text)

            if not tool_calls:
                break

            # Execute tools (OpenAI-compatible format)
            conv.append({"role": "assistant", "content": text,
                         "tool_calls": [{"id": tc["id"], "type": "function",
                                         "function": {"name": tc["name"],
                                                      "arguments": json.dumps(tc["arguments"])}}
                                        for tc in tool_calls]})
            for tc in tool_calls:
                tool_fn = TOOLS[tc["name"]]["fn"]
                with mlflow.start_span(name=f"autopilot:{tc['name']}") as span:
                    span.set_inputs(tc["arguments"])
                    result = tool_fn(**tc["arguments"])
                    span.set_outputs(result)
                conv.append({"role": "tool", "tool_call_id": tc["id"],
                             "content": json.dumps(result, default=str)})

        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content="\n".join(all_text), id=str(uuid.uuid4()))]
        )


# ─── Monitoring Loop ────────────────────────────────────────
def run_monitoring_loop(agent, interval_seconds=60, max_cycles=5, tracking_available=False):
    """Run the autopilot in a monitoring loop."""
    section("Autopilot Monitoring Loop")
    print(f"  Interval: {interval_seconds}s | Max cycles: {max_cycles}")

    for cycle in range(1, max_cycles + 1):
        print(f"\n{'═' * 60}")
        print(f"  Monitoring Cycle {cycle}/{max_cycles} — {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'═' * 60}")

        try:
            mlflow.end_run()
            run_ctx = mlflow.start_run(run_name=f"autopilot_cycle_{cycle}") if tracking_available else None
            if run_ctx:
                run_ctx.__enter__()
            response = agent.predict(messages=[
                ChatAgentMessage(
                    role="user",
                    content="Run a full health check. Collect metrics from all tables, "
                            "check streaming health, and take any needed maintenance actions. "
                            "Report your findings and any actions taken."
                )
            ])
            print(f"\n  Autopilot Report:")
            for msg in response.messages:
                for line in msg.content.split("\n"):
                    print(f"  {line}")
            if run_ctx:
                run_ctx.__exit__(None, None, None)
        except Exception as e:
            print(f"  Cycle {cycle} error: {e}")

        if cycle < max_cycles:
            print(f"\n  Next cycle in {interval_seconds}s...")
            time.sleep(interval_seconds)


# ─── Demo ───────────────────────────────────────────────────
def run_demo():
    section("MLflow Pipeline Autopilot Agent")

    tracking_available = False
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("overarchitected-autopilot")
        tracking_available = True
    except Exception:
        print("  MLflow tracking server not available — running without tracking.")

    if tracking_available:
        try:
            mlflow.openai.autolog()
        except Exception:
            pass

    agent = PipelineAutopilotAgent()

    print(f"\n  LLM Provider: {LLM_PROVIDER}")
    print(f"  LLM Model: {LLM_MODEL}")

    # Check if monitoring mode
    if "--monitor" in sys.argv:
        interval = 60
        for i, arg in enumerate(sys.argv):
            if arg == "--interval" and i + 1 < len(sys.argv):
                interval = int(sys.argv[i + 1])
        run_monitoring_loop(agent, interval_seconds=interval, max_cycles=10, tracking_available=tracking_available)
    else:
        # Single-shot demo
        demo_queries = [
            "Run a full health check on all lakehouse tables. Report any issues.",
            "Check streaming health and tell me if Kafka is accessible.",
            "Analyze the anomalies you found and execute any recommended maintenance actions.",
        ]

        for i, query in enumerate(demo_queries, 1):
            print(f"\n{'═' * 60}")
            print(f"  Autopilot Task {i}: {query}")
            print(f"{'═' * 60}")

            try:
                mlflow.end_run()
            run_ctx = mlflow.start_run(run_name=f"autopilot_demo_{i}") if tracking_available else None
                if run_ctx:
                    run_ctx.__enter__()
                response = agent.predict(
                    messages=[ChatAgentMessage(role="user", content=query)]
                )
                print(f"\n  Autopilot:")
                for msg in response.messages:
                    for line in msg.content.split("\n"):
                        print(f"  {line}")
                if run_ctx:
                    run_ctx.__exit__(None, None, None)
            except Exception as e:
                print(f"  Error: {e}")
                import traceback; traceback.print_exc()

    if tracking_available:
        section("Logging Autopilot to MLflow")
        try:
            with mlflow.start_run(run_name="autopilot_registration"):
                mlflow.pyfunc.log_model(
                    artifact_path="autopilot_agent",
                    python_model=agent,
                    registered_model_name="pipeline-autopilot",
                )
                print("  Autopilot logged as 'pipeline-autopilot'")
        except Exception as e:
            print(f"  Could not log model: {e}")


def main():
    print("\n" + "=" * 60)
    print("  PIPELINE AUTOPILOT")
    print("  Autonomous lakehouse management. The lazy dream.")
    print("=" * 60)

    run_demo()

    print("\n" + "=" * 60)
    print("  Done.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
