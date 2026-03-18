#!/usr/bin/env python3
"""
OverArchitected Act 6b: MLflow Data Quality Analyst Agent
==========================================================

Option B: Natural language → Spark SQL → results.
"What's the delivery performance in San Francisco this week?"

Architecture:
  User question → LLM generates SQL → Spark executes → LLM formats answer
  All traced via MLflow. Served via MLflow Agent Server.

Run:
    python scripts/demos/overarchitected/06b_mlflow_analyst.py

Prerequisites:
    pip install mlflow>=3.1 anthropic openai pyspark
    ./lakehouse start all
    ./lakehouse testdata generate --days 7
    ./lakehouse testdata load
"""

import os
import json
from datetime import datetime

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
LLM_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-4-5-20250514")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic")


def section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def get_spark():
    if SPARK_REMOTE:
        return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder \
        .master(SPARK_MASTER) \
        .appName("MLflow-Analyst-Agent") \
        .getOrCreate()


# ─── Schema Context ─────────────────────────────────────────
def get_schema_context():
    """Build schema context string for the LLM."""
    spark = get_spark()
    schemas = {}

    tables = [
        "iceberg.bronze.orders",
        "iceberg.bronze.dim_locations",
        "iceberg.bronze.dim_brands",
        "iceberg.bronze.dim_items",
        "iceberg.bronze.dim_categories",
    ]

    for table in tables:
        try:
            df = spark.table(table)
            cols = [(c.name, c.dataType.simpleString()) for c in df.schema]
            count = df.count()
            schemas[table] = {"columns": cols, "row_count": count}
        except Exception:
            pass

    context = "Available tables:\n"
    for table, info in schemas.items():
        context += f"\n{table} ({info['row_count']:,} rows):\n"
        for col_name, col_type in info["columns"]:
            context += f"  - {col_name}: {col_type}\n"

    context += """
Notes on the data:
- orders.ts is ISO 8601 string (e.g., '2024-01-15T12:00:00'). Use TO_TIMESTAMP(REPLACE(ts, 'T', ' ')) to convert.
- orders.body is a JSON string. Use get_json_object(body, '$.field') to extract fields.
- orders.event_type values: order_created, kitchen_started, kitchen_finished, order_ready, driver_arrived, driver_picked_up, driver_ping, delivered
- orders.sequence: 0 for order_created, increases through lifecycle
- body fields vary by event_type:
  - order_created: brand_id, total, items (array)
  - delivered: delivery_lat, delivery_lon, total_mins
  - driver_ping: lat, lon, progress_pct
- dim_locations: id, city, lat, lon
- dim_brands: id, name, cuisine_type, avg_prep_time_mins, momentum
"""
    return context


SYSTEM_PROMPT_TEMPLATE = """You are a Data Quality Analyst for a food delivery lakehouse (Casper's Kitchen).
You answer questions about the data by writing and executing Spark SQL queries.

{schema_context}

Rules:
1. Always use the query_lakehouse tool to run SQL — never guess at data.
2. Write efficient SQL — use LIMIT, avoid SELECT *.
3. For timestamps, always use: TO_TIMESTAMP(REPLACE(ts, 'T', ' '))
4. For JSON extraction, use: get_json_object(body, '$.field')
5. Be concise and technical. Include the SQL you ran in your answer.
6. If a query fails, explain why and try a different approach.
"""


# ─── Tool ───────────────────────────────────────────────────
def query_lakehouse(sql: str) -> dict:
    """Execute SQL against the lakehouse and return results."""
    spark = get_spark()
    result = {"sql": sql}

    try:
        df = spark.sql(sql)
        rows = df.limit(50).collect()
        result["columns"] = df.columns
        result["row_count"] = len(rows)
        result["data"] = [row.asDict() for row in rows]
    except Exception as e:
        result["error"] = str(e)

    return result


TOOL_SCHEMAS = [{
    "type": "function",
    "function": {
        "name": "query_lakehouse",
        "description": "Execute a Spark SQL query against the Iceberg lakehouse. Returns up to 50 rows.",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "The SQL query to execute"}
            },
            "required": ["sql"]
        }
    }
}]


# ─── Agent ──────────────────────────────────────────────────
class DataAnalystAgent(ChatAgent):
    """Natural language → SQL → answer agent."""

    def __init__(self):
        self._client = None
        self._schema_context = None

    @property
    def schema_context(self):
        if self._schema_context is None:
            self._schema_context = get_schema_context()
        return self._schema_context

    @property
    def client(self):
        if self._client is None:
            if LLM_PROVIDER == "anthropic":
                import anthropic
                self._client = anthropic.Anthropic()
            else:
                import openai
                base_url = os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1")
                self._client = openai.OpenAI(base_url=base_url, api_key="ollama")
        return self._client

    def _call_llm(self, messages, tools=None):
        system = SYSTEM_PROMPT_TEMPLATE.format(schema_context=self.schema_context)

        if LLM_PROVIDER == "anthropic":
            api_messages = [m for m in messages if m["role"] != "system"]
            kwargs = {"model": LLM_MODEL, "max_tokens": 4096, "system": system, "messages": api_messages}
            if tools:
                kwargs["tools"] = [
                    {"name": t["function"]["name"], "description": t["function"]["description"],
                     "input_schema": t["function"]["parameters"]}
                    for t in tools
                ]
            return self.client.messages.create(**kwargs)
        else:
            kwargs = {
                "model": LLM_MODEL, "max_tokens": 4096,
                "messages": [{"role": "system", "content": system}] + messages,
            }
            if tools:
                kwargs["tools"] = tools
            return self.client.chat.completions.create(**kwargs)

    def _extract_response(self, response):
        if LLM_PROVIDER == "anthropic":
            text_parts, tool_calls = [], []
            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_calls.append({"id": block.id, "name": block.name, "arguments": block.input})
            return "\n".join(text_parts), tool_calls
        else:
            msg = response.choices[0].message
            tool_calls = []
            if msg.tool_calls:
                for tc in msg.tool_calls:
                    tool_calls.append({"id": tc.id, "name": tc.function.name,
                                       "arguments": json.loads(tc.function.arguments)})
            return msg.content or "", tool_calls

    @mlflow.trace
    def predict(self, messages, params=None):
        conv = [{"role": m.role, "content": m.content} for m in messages]
        all_text = []

        for _ in range(8):
            response = self._call_llm(conv, tools=TOOL_SCHEMAS)
            text, tool_calls = self._extract_response(response)

            if text:
                all_text.append(text)

            if not tool_calls:
                break

            # Execute tools
            if LLM_PROVIDER == "anthropic":
                assistant_content = []
                if text:
                    assistant_content.append({"type": "text", "text": text})
                for tc in tool_calls:
                    assistant_content.append({"type": "tool_use", "id": tc["id"],
                                              "name": tc["name"], "input": tc["arguments"]})
                conv.append({"role": "assistant", "content": assistant_content})

                for tc in tool_calls:
                    with mlflow.start_span(name=f"sql:{tc['name']}") as span:
                        span.set_inputs(tc["arguments"])
                        result = query_lakehouse(**tc["arguments"])
                        span.set_outputs(result)
                    conv.append({"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": tc["id"],
                         "content": json.dumps(result, default=str)}
                    ]})
            else:
                conv.append({"role": "assistant", "content": text,
                             "tool_calls": [{"id": tc["id"], "type": "function",
                                             "function": {"name": tc["name"],
                                                          "arguments": json.dumps(tc["arguments"])}}
                                            for tc in tool_calls]})
                for tc in tool_calls:
                    with mlflow.start_span(name=f"sql:{tc['name']}") as span:
                        span.set_inputs(tc["arguments"])
                        result = query_lakehouse(**tc["arguments"])
                        span.set_outputs(result)
                    conv.append({"role": "tool", "tool_call_id": tc["id"],
                                 "content": json.dumps(result, default=str)})

        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content="\n".join(all_text))]
        )


# ─── Demo ───────────────────────────────────────────────────
def run_demo():
    section("MLflow Data Quality Analyst Agent")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    try:
        mlflow.set_experiment("overarchitected-analyst")
    except Exception:
        pass

    if LLM_PROVIDER == "anthropic":
        try:
            mlflow.anthropic.autolog()
        except Exception:
            pass

    agent = DataAnalystAgent()

    demo_queries = [
        "What's the total order volume by city? Which city has the most orders?",
        "What are the peak ordering hours? Show me the hourly distribution.",
        "Which brands have the highest average order value? Top 5.",
        "What percentage of orders have null or missing body data?",
        "What's the average delivery time by city? Which city is fastest?",
    ]

    print(f"\n  LLM Provider: {LLM_PROVIDER}")
    print(f"  LLM Model: {LLM_MODEL}")

    for i, query in enumerate(demo_queries, 1):
        print(f"\n{'═' * 60}")
        print(f"  Question {i}: {query}")
        print(f"{'═' * 60}")

        try:
            with mlflow.start_run(run_name=f"analyst_demo_{i}"):
                response = agent.predict(
                    messages=[ChatAgentMessage(role="user", content=query)]
                )
                print(f"\n  Answer:")
                print(f"  {'-' * 50}")
                for msg in response.messages:
                    for line in msg.content.split("\n"):
                        print(f"  {line}")
        except Exception as e:
            print(f"  Error: {e}")

    # Log model
    section("Logging Agent to MLflow")
    try:
        with mlflow.start_run(run_name="analyst_agent_registration"):
            mlflow.pyfunc.log_model(
                artifact_path="analyst_agent",
                python_model=agent,
                registered_model_name="data-analyst",
            )
            print("  Agent logged as 'data-analyst'")
    except Exception as e:
        print(f"  Could not log model: {e}")


def main():
    print("\n" + "=" * 60)
    print("  ACT 6b: DATA QUALITY ANALYST AGENT")
    print("  Ask questions in English. Get SQL + answers.")
    print("=" * 60)

    run_demo()

    print("\n" + "=" * 60)
    print("  Act 6b complete.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
