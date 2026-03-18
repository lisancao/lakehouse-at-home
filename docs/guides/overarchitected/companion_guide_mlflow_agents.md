# Companion Guide: MLflow AI Agents for Lakehouse Management

**OverArchitected Show — Act 6: "We're Lazy"**

| | |
|---|---|
| **Audience** | Data engineers, platform engineers, and ML engineers familiar with MLflow for experiment tracking who want to understand the 3.x agentic capabilities |
| **Complements** | Demo scripts `06a_mlflow_guardian.py`, `06b_mlflow_analyst.py`, `06c_mlflow_autopilot.py` |
| **Prerequisites** | Basic familiarity with MLflow concepts (experiments, runs, models), Python, and PySpark |
| **Stack versions** | MLflow 3.1+, Spark 4.1, Iceberg 1.10, Python 3.10+ |

---

## Table of Contents

1. [Introduction: MLflow Is Not What You Remember](#1-introduction-mlflow-is-not-what-you-remember)
2. [MLflow 3.x Overview](#2-mlflow-3x-overview)
3. [MLflow Tracing](#3-mlflow-tracing)
4. [Agent Interfaces: ChatAgent and ResponsesAgent](#4-agent-interfaces-chatagent-and-responsesagent)
5. [MLflow Agent Server](#5-mlflow-agent-server)
6. [MLflow AI Gateway](#6-mlflow-ai-gateway)
7. [Unity Catalog Model Registry Integration](#7-unity-catalog-model-registry-integration)
8. [The Three Agent Patterns](#8-the-three-agent-patterns)
9. [Ollama as OSS LLM Fallback](#9-ollama-as-oss-llm-fallback)
10. [OSS vs Databricks MLflow](#10-oss-vs-databricks-mlflow)
11. [Practical Setup](#11-practical-setup)
12. [References](#12-references)

---

## 1. Introduction: MLflow Is Not What You Remember

If you last touched MLflow in the 1.x or 2.x era, you know it as a tool for logging model parameters, metrics, and artifacts. It was the de facto standard for ML experiment tracking -- reliable, straightforward, and narrowly scoped.

MLflow 3.x is a different product.

Starting with version 2.15 (mid-2024) and accelerating through the 3.0 release (early 2025), MLflow has repositioned itself as a **GenAI-native platform** for building, evaluating, deploying, and monitoring AI agents. The tracking server you already know is still there, but it is now surrounded by:

- **Tracing infrastructure** compatible with OpenTelemetry, with auto-instrumentation for 40+ LLM frameworks and providers
- **First-class agent interfaces** (`ChatAgent`, `ResponsesAgent`) that package any LLM-calling code into a deployable, traceable unit
- **An embedded AI Gateway** that routes LLM traffic, enforces rate limits, supports fallback chains, and tracks spend
- **An Agent Server** (FastAPI-based) that serves agents over HTTP with streaming, tool calling, and Docker packaging
- **Deep Unity Catalog integration** for governed model lifecycle across catalogs, schemas, and environments

The thesis is simple: the same infrastructure that manages ML models should manage AI agents. An agent is, after all, a model with tools. MLflow 3.x provides the rails for building agents in any framework (or no framework), deploying them as services, tracing every LLM call and tool invocation, and governing the whole lifecycle through a model registry.

This companion guide covers every MLflow 3.x capability demonstrated in Act 6 of the OverArchitected show, plus the broader context you need to use these features in production. Every claim is cited with links to official documentation or source code.

> **Why this matters for data engineers:** You already operate Spark, Iceberg, Kafka, and Airflow. MLflow 3.x lets you build agents that operate *on* that infrastructure -- querying tables, running maintenance, monitoring pipelines -- with full observability and governance. No new orchestrator needed. No vendor lock-in.

---

## 2. MLflow 3.x Overview

### 2.1 The Evolution: Tracking Tool to AI Platform

MLflow's journey from experiment tracker to AI engineering platform happened in phases:

| Version | Date | Key Addition |
|---------|------|-------------|
| 1.0 | Jun 2018 | Tracking, Projects, Models, Model Registry |
| 2.0 | Nov 2022 | MLflow Recipes, enhanced model signatures |
| 2.8 | Nov 2023 | LLM evaluation (`mlflow.evaluate()` with GenAI metrics) |
| 2.11 | Mar 2024 | MLflow Tracing (preview), auto-logging for OpenAI |
| 2.15 | Aug 2024 | AI Gateway embedded in tracking server, expanded tracing |
| 2.17 | Oct 2024 | `ChatAgent` interface, agent evaluation |
| 3.0 | Jan 2025 | `LoggedModel` as first-class entity, GenAI-native redesign |
| 3.1 | Feb 2025 | `ResponsesAgent` (Responses API schema), enhanced tracing |
| 3.5 | Mar 2025 | Async tracing, production-ready Gateway |
| 3.9 | Jun 2025 | Gateway hot-reload, budget alerts, traffic splitting |

Sources:
- [MLflow Changelog](https://github.com/mlflow/mlflow/blob/master/CHANGELOG.md)
- [MLflow 3.0 Release Blog](https://mlflow.org/blog/mlflow-3-0)
- [MLflow Releases on PyPI](https://pypi.org/project/mlflow/#history)

### 2.2 LoggedModel: The First-Class Entity

Before MLflow 3.0, models existed as artifacts attached to runs. You logged a model inside a run, and its identity was derived from the run. This created friction: the same model trained in different runs was hard to track as a single lineage.

MLflow 3.0 introduced `LoggedModel` as a **top-level entity**, independent of runs:

```python
import mlflow

# Create a LoggedModel (exists independent of any run)
model = mlflow.create_logged_model(
    name="pipeline-guardian",
    model_type="agent",       # "agent", "llm", "classifier", "regressor", etc.
    source_run_id=run.info.run_id,
)

# Log artifacts, metrics, tags to the model directly
mlflow.log_model_artifact(model.model_id, artifact_path="config.yaml", local_path="./config.yaml")
mlflow.set_model_tag(model.model_id, "framework", "custom")
```

Key properties of `LoggedModel`:
- **model_id**: A unique UUID, stable across versions
- **model_type**: Declares the model category (agent, LLM, retriever, classifier, etc.)
- **source_run_id**: Links back to the training/creation run
- **creation_timestamp**: Immutable creation time
- **tags**: Key-value metadata

This matters because agents are not trained in the traditional sense -- they are *assembled* from prompts, tools, and LLM configurations. `LoggedModel` gives them first-class identity without requiring a training run.

Source: [MLflow LoggedModel documentation](https://mlflow.org/docs/latest/api_reference/mlflow/entities.html#mlflow.entities.LoggedModel)

### 2.3 GenAI-Native Platform

MLflow 3.x reframes the entire product around generative AI workflows:

| Traditional ML (MLflow 1.x-2.x) | GenAI (MLflow 3.x) |
|---|---|
| Log parameters, metrics, artifacts per training run | Log traces per inference call |
| Model = serialized artifact (pickle, ONNX, etc.) | Model = code + config + tools (often no serialization) |
| Evaluate against test datasets with sklearn metrics | Evaluate with LLM-as-judge, human feedback, custom scorers |
| Deploy as batch or REST endpoint | Deploy as streaming agent with tool-calling loops |
| Registry tracks model versions | Registry tracks model versions + agent versions + prompt versions |

The practical impact: you can build a LangChain agent, a raw OpenAI function-calling loop, or a custom Python class, and MLflow treats them all the same way -- loggable, traceable, servable, evaluatable.

### 2.4 Scale: 30+ Million Monthly Downloads

MLflow crossed 30 million monthly downloads on PyPI in late 2024, making it the most widely adopted open-source ML platform. The 3.x GenAI features ride on this existing adoption -- teams do not need to adopt a new tool; they extend one they already use.

Source: [PyPI Download Statistics for MLflow](https://pypistats.org/packages/mlflow)

---

## 3. MLflow Tracing

### 3.1 What Tracing Is and Why It Matters

Every LLM call is a black box: you send a prompt, tokens go in, tokens come out. In an agent with tool-calling loops, a single user request can trigger 5-15 LLM calls, each with different prompts, tool schemas, and results. Without tracing, debugging is guesswork.

MLflow Tracing captures the full execution graph of an agent interaction:

```
User request: "Check health of all bronze tables"
  └─ Agent.predict()                              [trace root]
       ├─ LLM call #1 (Claude)                    [span: llm]
       │   ├─ Input: system prompt + user message
       │   ├─ Output: tool_use(inspect_table, iceberg.bronze.orders)
       │   ├─ Tokens: 1,247 input / 89 output
       │   └─ Cost: $0.0041
       ├─ Tool: inspect_table                      [span: tool]
       │   ├─ Input: {table_name: "iceberg.bronze.orders"}
       │   └─ Output: {row_count: 14283, file_count: 47, ...}
       ├─ LLM call #2 (Claude)                    [span: llm]
       │   ├─ Input: conversation + tool result
       │   ├─ Output: tool_use(inspect_table, iceberg.bronze.dim_locations)
       │   └─ Cost: $0.0063
       ├─ Tool: inspect_table                      [span: tool]
       │   └─ Output: {row_count: 25, file_count: 1, ...}
       ├─ LLM call #3 (Claude)                    [span: llm]
       │   ├─ Output: text summary of findings
       │   └─ Cost: $0.0089
       └─ Total: 3 LLM calls, 2 tool calls, $0.0193, 4.2s
```

Each node in this tree is a **span**. The entire tree is a **trace**. Traces are stored in the MLflow tracking server and viewable in the UI.

### 3.2 OpenTelemetry Compatibility

MLflow Tracing is built on the [OpenTelemetry](https://opentelemetry.io/) standard (OTel). This means:

- Traces use the OTel span format (trace ID, span ID, parent span ID, attributes, events)
- You can export MLflow traces to any OTel-compatible backend (Jaeger, Zipkin, Datadog, Grafana Tempo)
- You can import OTel traces from other services into MLflow
- The `mlflow.tracing` module implements the OTel `TracerProvider` interface

```python
# Export traces to an OTel collector alongside MLflow
from opentelemetry.sdk.trace.export import BatchSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# MLflow traces are automatically OTel-compatible
# Configure an additional exporter if needed:
import mlflow
mlflow.tracing.set_span_exporter(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
```

Source: [MLflow Tracing — OpenTelemetry compatibility](https://mlflow.org/docs/latest/llms/tracing/index.html#opentelemetry-compatibility)

### 3.3 Auto-Tracing: 40+ Integrations

The killer feature of MLflow Tracing is **auto-tracing** -- one line of code instruments an entire library. As of MLflow 3.1, supported integrations include:

| Category | Libraries | Setup |
|----------|-----------|-------|
| **LLM Providers** | OpenAI, Anthropic, Google Gemini, Amazon Bedrock, Azure OpenAI, Mistral AI, Cohere | `mlflow.<provider>.autolog()` |
| **Agent Frameworks** | LangChain, LangGraph, LlamaIndex, CrewAI, AutoGen, Haystack, AG2 | `mlflow.<framework>.autolog()` |
| **OSS Inference** | Ollama, vLLM, HuggingFace Transformers | `mlflow.<library>.autolog()` |
| **Tool/RAG** | DSPy, Instructor, LiteLLM | `mlflow.<library>.autolog()` |

For our demo agents, the setup is one line:

```python
# Instrument all Anthropic API calls automatically
mlflow.anthropic.autolog()

# Or if using Ollama via OpenAI client:
mlflow.openai.autolog()
```

After this line, every call to `anthropic.Anthropic().messages.create()` or `openai.OpenAI().chat.completions.create()` is automatically captured as a trace span with:
- Full request (model, messages, tools, temperature, etc.)
- Full response (content, tool calls, stop reason)
- Token counts (input, output, total)
- Latency
- Cost estimation (based on published pricing)

Source: [MLflow Auto-Tracing Integrations](https://mlflow.org/docs/latest/llms/tracing/index.html#automatic-tracing)

### 3.4 Manual Tracing: `@mlflow.trace` and `mlflow.start_span()`

For custom code (tool functions, data transformations, business logic), use manual tracing:

```python
import mlflow

# Decorator-based: traces the entire function
@mlflow.trace
def predict(self, messages, params=None):
    """This creates a root span named 'predict'."""
    # ... agent logic ...

# Context-manager-based: traces a specific block
with mlflow.start_span(name="tool:inspect_table") as span:
    span.set_inputs({"table_name": "iceberg.bronze.orders"})
    result = inspect_table("iceberg.bronze.orders")
    span.set_outputs(result)
```

Span attributes you can set:

| Method | Purpose |
|--------|---------|
| `span.set_inputs(dict)` | Record function inputs |
| `span.set_outputs(dict)` | Record function outputs |
| `span.set_attribute(key, value)` | Set arbitrary metadata |
| `span.set_status(status)` | Mark span as OK or ERROR |
| `span.add_event(name, attributes)` | Add timestamped events within the span |

This is exactly how the demo agents trace tool calls:

```python
# From 06a_mlflow_guardian.py
for tc in tool_calls:
    tool_fn = TOOLS[tc["name"]]["fn"]
    with mlflow.start_span(name=f"tool:{tc['name']}") as span:
        span.set_inputs(tc["arguments"])
        result = tool_fn(**tc["arguments"])
        span.set_outputs(result)
```

Source: [MLflow Manual Tracing API](https://mlflow.org/docs/latest/llms/tracing/index.html#manual-tracing)

### 3.5 Production-Ready Async Logging

In production, you do not want trace logging to block your agent's response. MLflow 3.5+ introduced async trace logging:

```python
# Enable async logging (default in MLflow 3.5+)
mlflow.config.enable_async_logging()

# Traces are buffered and sent to the tracking server asynchronously
# No impact on agent response latency
```

Configuration options:

| Setting | Default | Description |
|---------|---------|-------------|
| `MLFLOW_ENABLE_ASYNC_LOGGING` | `true` (3.5+) | Enable/disable async |
| `MLFLOW_ASYNC_LOGGING_BUFFERING_TIMEOUT_SECONDS` | `1` | Max time to buffer before flush |
| `MLFLOW_ASYNC_LOGGING_THREADPOOL_SIZE` | `10` | Number of worker threads |

Source: [MLflow Async Logging](https://mlflow.org/docs/latest/llms/tracing/index.html#async-logging)

### 3.6 Cost and Token Tracking

MLflow Tracing automatically extracts token counts and computes costs from LLM responses:

```python
# After a traced call, the span contains:
span.attributes["mlflow.completionTokens"]    # Output tokens
span.attributes["mlflow.promptTokens"]         # Input tokens
span.attributes["mlflow.totalTokens"]          # Total tokens
```

Cost computation uses published provider pricing tables. You can override with custom pricing:

```python
mlflow.tracing.set_token_pricing(
    model="claude-sonnet-4-5-20250514",
    input_cost_per_1k=0.003,
    output_cost_per_1k=0.015,
)
```

In the MLflow UI, the Traces tab shows per-trace and per-span token counts, costs, and latencies. This is critical for understanding agent economics -- a single "check all tables" request might trigger 5-10 LLM calls costing $0.02-0.10.

Source: [MLflow Tracing — Token and Cost Tracking](https://mlflow.org/docs/latest/llms/tracing/index.html#token-usage-and-cost-tracking)

### 3.7 Viewing Traces in the UI

The MLflow tracking UI (http://localhost:5000) includes a dedicated **Traces** tab:

```
MLflow UI → Experiments → overarchitected-guardian → Traces tab
  └─ Trace: guardian_demo_1 (4.2s, $0.019)
       ├─ predict [root span]
       ├─ anthropic.messages.create [auto-traced, 1.1s]
       ├─ tool:inspect_table [manual span, 0.8s]
       ├─ anthropic.messages.create [auto-traced, 1.0s]
       ├─ tool:inspect_table [manual span, 0.6s]
       └─ anthropic.messages.create [auto-traced, 0.7s]
```

Each span is expandable, showing full inputs/outputs. This makes debugging agent behavior straightforward: you can see exactly what the LLM was asked, what it decided, what the tool returned, and what it concluded.

---

## 4. Agent Interfaces: ChatAgent and ResponsesAgent

### 4.1 The Problem: Framework Fragmentation

Every LLM framework has its own interface:
- LangChain: `AgentExecutor.invoke({"input": "..."})`
- LlamaIndex: `QueryEngine.query("...")`
- OpenAI Assistants: `client.beta.threads.messages.create(...)`
- Raw SDK: `client.messages.create(model=..., messages=[...])`

This makes it impossible to build generic tooling (evaluation, serving, monitoring) that works across frameworks. MLflow solves this with two standard agent interfaces.

### 4.2 ChatAgent (ChatCompletion Schema)

`ChatAgent` is the original MLflow agent interface, introduced in MLflow 2.17. It uses the **ChatCompletion** message schema (matching OpenAI's chat completion API):

```python
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentMessage,
    ChatAgentParams,
    ChatAgentResponse,
    ChatAgentChunk,
)

class MyAgent(ChatAgent):
    """An agent that follows the ChatCompletion message schema."""

    def predict(self, messages: list[ChatAgentMessage], params: ChatAgentParams = None) -> ChatAgentResponse:
        """
        Required method. Receives a list of messages, returns a response.

        Args:
            messages: List of ChatAgentMessage with .role and .content
            params: Optional parameters (temperature, max_tokens, etc.)

        Returns:
            ChatAgentResponse containing one or more ChatAgentMessage objects
        """
        # Your agent logic here
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content="Hello!")]
        )

    def predict_stream(self, messages, params=None):
        """
        Optional. For streaming responses.
        Yields ChatAgentChunk objects.
        """
        yield ChatAgentChunk(delta=ChatAgentMessage(role="assistant", content="Hel"))
        yield ChatAgentChunk(delta=ChatAgentMessage(role="assistant", content="lo!"))
```

The `ChatAgentMessage` schema:

| Field | Type | Description |
|-------|------|-------------|
| `role` | str | "user", "assistant", "system", or "tool" |
| `content` | str or None | Text content |
| `name` | str or None | Tool name (for role="tool") |
| `tool_calls` | list or None | Tool call requests (for role="assistant") |
| `tool_call_id` | str or None | ID linking tool result to tool call |

This is the interface used in all three Act 6 demo agents:

```python
# From 06a_mlflow_guardian.py
class PipelineGuardianAgent(ChatAgent):
    @mlflow.trace
    def predict(self, messages, params=None):
        conv = [{"role": m.role, "content": m.content} for m in messages]
        # ... tool-calling loop ...
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=final_text)]
        )
```

Source: [MLflow ChatAgent documentation](https://mlflow.org/docs/latest/llms/chat-agent/index.html)

### 4.3 ResponsesAgent (Responses API Schema) -- Recommended

`ResponsesAgent` was introduced in MLflow 3.1, aligning with OpenAI's newer **Responses API** schema. It is now the recommended interface for new agents:

```python
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.agent import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

class MyResponsesAgent(ResponsesAgent):
    """An agent using the Responses API schema."""

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """
        Required method.

        Args:
            request: ResponsesAgentRequest with:
                - input: str or list of input items
                - model: optional model name
                - instructions: optional system instructions
                - tools: optional list of tool definitions
                - temperature: optional float
                - max_output_tokens: optional int

        Returns:
            ResponsesAgentResponse with:
                - output: list of output items (text, tool calls, etc.)
                - model: model used
                - usage: token counts
        """
        return ResponsesAgentResponse(
            output=[{"type": "message", "role": "assistant",
                     "content": [{"type": "output_text", "text": "Hello!"}]}],
        )

    def predict_stream(self, request):
        """Optional. Yields ResponsesAgentStreamEvent objects."""
        yield ResponsesAgentStreamEvent(type="response.output_text.delta", delta="Hel")
        yield ResponsesAgentStreamEvent(type="response.output_text.delta", delta="lo!")
```

Key differences from `ChatAgent`:

| Aspect | ChatAgent | ResponsesAgent |
|--------|-----------|----------------|
| Message format | ChatCompletion (messages list) | Responses API (input items) |
| Tool definitions | External (you manage) | Inline in request |
| Streaming events | ChatAgentChunk | ResponsesAgentStreamEvent |
| Multi-turn | Managed by caller | Can include previous_response_id |
| Recommended for | Legacy, simple chat | New projects, complex agents |

Source: [MLflow ResponsesAgent documentation](https://mlflow.org/docs/latest/llms/responses-agent/index.html)

### 4.4 Framework-Agnostic Wrapping

The power of these interfaces is that they wrap *anything*. Your agent logic can use:

- Raw Anthropic/OpenAI SDK calls (as in our demo agents)
- LangChain/LangGraph chains
- LlamaIndex query engines
- Custom Python with no framework at all
- Even subprocess calls to CLI tools

As long as your class implements `predict()` (and optionally `predict_stream()`), MLflow can:
1. **Log it** as a model artifact
2. **Serve it** via the Agent Server
3. **Trace it** automatically
4. **Evaluate it** with `mlflow.evaluate()`
5. **Register it** in the model registry (including Unity Catalog)

```python
# Wrap a LangGraph agent in ChatAgent
from langchain_core.messages import HumanMessage

class LangGraphWrapper(ChatAgent):
    def __init__(self):
        self.graph = build_my_langgraph()  # Your LangGraph definition

    def predict(self, messages, params=None):
        # Convert MLflow messages to LangChain messages
        lc_messages = [HumanMessage(content=m.content) for m in messages if m.role == "user"]
        result = self.graph.invoke({"messages": lc_messages})
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=result["messages"][-1].content)]
        )
```

Source: [MLflow Custom Agents](https://mlflow.org/docs/latest/llms/chat-agent/index.html#custom-chatagent)

---

## 5. MLflow Agent Server

### 5.1 What It Is

The MLflow Agent Server is a **FastAPI-based HTTP server** that serves any logged MLflow model (including agents) over REST and streaming endpoints. It replaces the older `mlflow models serve` for agent use cases, though `mlflow models serve` still works.

```bash
# Serve a logged agent model
mlflow models serve -m runs:/<run_id>/guardian_agent -p 5001

# Or from the model registry
mlflow models serve -m models:/pipeline-guardian/latest -p 5001

# Development mode with auto-reload
mlflow models serve -m runs:/<run_id>/guardian_agent -p 5001 --enable-mlserver --reload
```

### 5.2 The `/invocations` Endpoint

The primary endpoint is `POST /invocations`, which accepts the agent's input schema:

**For ChatAgent:**

```bash
curl -X POST http://localhost:5001/invocations \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "Check the health of all bronze tables"}
    ]
  }'
```

Response:

```json
{
  "messages": [
    {
      "role": "assistant",
      "content": "I've inspected all 5 bronze tables. Here are the findings:\n\n**iceberg.bronze.orders**: 14,283 rows, 47 files (127 MB)..."
    }
  ]
}
```

**For ResponsesAgent:**

```bash
curl -X POST http://localhost:5001/invocations \
  -H "Content-Type: application/json" \
  -d '{
    "input": "Check the health of all bronze tables",
    "instructions": "Be concise and technical."
  }'
```

### 5.3 Streaming Support

For agents that support `predict_stream()`, the `/invocations` endpoint supports server-sent events (SSE) when the `Accept: text/event-stream` header is set:

```bash
curl -X POST http://localhost:5001/invocations \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "messages": [
      {"role": "user", "content": "What are the peak ordering hours?"}
    ]
  }'
```

Response (SSE stream):

```
data: {"delta": {"role": "assistant", "content": "Let me "}}

data: {"delta": {"role": "assistant", "content": "query the "}}

data: {"delta": {"role": "assistant", "content": "orders table..."}}

data: [DONE]
```

### 5.4 Docker Packaging

MLflow can package any served model into a Docker image:

```bash
# Build a Docker image for the agent
mlflow models build-docker \
  -m models:/pipeline-guardian/latest \
  -n lakehouse-guardian:latest

# Run the container
docker run -p 5001:8080 \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  -e MLFLOW_TRACKING_URI=http://host.docker.internal:5000 \
  lakehouse-guardian:latest
```

The generated Docker image:
- Uses a slim Python base image
- Includes all model dependencies (from the logged `conda.yaml` or `requirements.txt`)
- Exposes port 8080 by default
- Runs the MLflow model server internally
- Supports health checks at `/health`

This is how you deploy agents to Kubernetes, ECS, Cloud Run, or any container orchestrator.

Source: [MLflow Models — Docker](https://mlflow.org/docs/latest/deployment/deploy-model-locally.html#building-a-docker-image)

### 5.5 Development Mode

During development, use `--reload` to auto-restart the server when code changes:

```bash
# Development mode — watches for file changes
mlflow models serve \
  -m runs:/<run_id>/guardian_agent \
  -p 5001 \
  --reload

# The server restarts automatically when you modify the agent code
```

Additional development options:

| Flag | Purpose |
|------|---------|
| `--reload` | Auto-restart on code changes |
| `--workers N` | Number of uvicorn workers (default: 1) |
| `--timeout-keep-alive N` | Keep-alive timeout in seconds |
| `--host HOST` | Bind address (default: 127.0.0.1) |
| `--port PORT` | Bind port (default: 5000) |
| `--no-conda` | Skip conda environment creation |

Source: [MLflow Models — Local Serving](https://mlflow.org/docs/latest/deployment/deploy-model-locally.html)

---

## 6. MLflow AI Gateway

### 6.1 What the Gateway Does

The MLflow AI Gateway is a **reverse proxy for LLM APIs**. Instead of your agents calling Anthropic, OpenAI, or Ollama directly, they call the Gateway, which routes requests to the appropriate provider. This gives you:

- **Unified endpoint**: One URL for all LLM calls, regardless of provider
- **Provider abstraction**: Switch from Claude to GPT-4 by changing a YAML config, not code
- **Rate limiting**: Enforce calls-per-minute limits per endpoint
- **Traffic splitting**: Send 80% of traffic to Claude, 20% to GPT-4 for comparison
- **Fallback chains**: If Claude is down, automatically fall back to Ollama
- **Cost tracking**: Log all LLM traffic with token counts and estimated costs
- **Hot-reloadable config**: Change routing without restarting the server

### 6.2 Embedded in the Tracking Server (Since MLflow 2.15 / 3.9+)

The Gateway is not a separate service. It is **embedded in the MLflow tracking server**. When you start `mlflow server` with a `MLFLOW_GATEWAY_CONFIG` environment variable, the Gateway starts alongside the tracking UI:

```bash
# The docker-compose-mlflow.yml already configures this:
MLFLOW_GATEWAY_CONFIG: "/config/gateway-config.yml"
```

The Gateway endpoints are available at:
- `http://localhost:5000/gateway/chat/invocations` — for the "chat" endpoint
- `http://localhost:5000/gateway/chat-local/invocations` — for the "chat-local" endpoint
- `http://localhost:5000/gateway/` — Gateway management API

Source: [MLflow AI Gateway](https://mlflow.org/docs/latest/llms/gateway/index.html)

### 6.3 YAML Configuration (Hot-Reloadable)

The Gateway is configured via YAML. Here is the full configuration used in this project (`config/mlflow/gateway-config.yml`):

```yaml
# MLflow AI Gateway Configuration
# Hot-reloadable — changes take effect without restart.

endpoints:
  # Primary: Anthropic Claude
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}

  # Fallback: Ollama (local OSS model)
  - name: chat-local
    endpoint_type: llm/v1/chat
    model:
      provider: openai
      name: qwen2.5:14b
      config:
        openai_api_base: http://localhost:11434/v1
        openai_api_key: "ollama"

  # Completions (for non-chat use cases)
  - name: completions
    endpoint_type: llm/v1/completions
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}
```

Configuration is hot-reloadable: edit the YAML file and the Gateway picks up changes without a server restart. This is critical for production -- you can add a new provider, change rate limits, or switch fallback targets without downtime.

### 6.4 Supported Endpoint Types

| Endpoint Type | Description | Input Schema |
|---------------|-------------|-------------|
| `llm/v1/chat` | Chat completion (messages in, message out) | OpenAI ChatCompletion format |
| `llm/v1/completions` | Text completion (prompt in, text out) | OpenAI Completion format |
| `llm/v1/embeddings` | Text embeddings (text in, vectors out) | OpenAI Embeddings format |

### 6.5 Supported Providers

| Provider | Config Key | Notes |
|----------|-----------|-------|
| **Anthropic** | `anthropic_api_key` | Claude models (Haiku, Sonnet, Opus) |
| **OpenAI** | `openai_api_key` | GPT-4, GPT-3.5, o1, etc. |
| **Azure OpenAI** | `openai_api_key`, `openai_api_base`, `openai_deployment_name` | Azure-hosted OpenAI models |
| **Google** | `google_api_key` | Gemini models |
| **Amazon Bedrock** | `aws_config` | All Bedrock-supported models |
| **Mistral AI** | `mistral_api_key` | Mistral, Mixtral models |
| **Cohere** | `cohere_api_key` | Command models |
| **AI21 Labs** | `ai21_api_key` | Jurassic models |
| **Ollama** | Uses OpenAI provider with custom `openai_api_base` | Any Ollama model |
| **vLLM** | Uses OpenAI provider with custom `openai_api_base` | Self-hosted inference |
| **HuggingFace TGI** | Uses OpenAI provider with custom `openai_api_base` | Text Generation Inference |

Source: [MLflow AI Gateway — Supported Providers](https://mlflow.org/docs/latest/llms/gateway/index.html#supported-provider-models)

### 6.6 Rate Limiting

Rate limiting is configured per endpoint:

```yaml
endpoints:
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}
    limit:
      calls: 100
      renewal_period: minute
```

Rate limit options:

| Field | Values | Description |
|-------|--------|-------------|
| `calls` | integer | Max calls in renewal period |
| `renewal_period` | `second`, `minute`, `hour` | Window for rate limit |

When the limit is hit, the Gateway returns HTTP 429 with a `Retry-After` header.

### 6.7 Traffic Splitting

Send traffic across multiple providers for A/B testing or gradual rollout:

```yaml
endpoints:
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}
    traffic_split:
      - model:
          provider: anthropic
          name: claude-sonnet-4-5-20250514
        weight: 80
      - model:
          provider: openai
          name: gpt-4o
          config:
            openai_api_key: ${OPENAI_API_KEY}
        weight: 20
```

### 6.8 Fallback Chains

If the primary provider fails (timeout, rate limit, error), automatically fall back:

```yaml
endpoints:
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}
    fallback:
      - model:
          provider: openai
          name: qwen2.5:14b
          config:
            openai_api_base: http://localhost:11434/v1
            openai_api_key: "ollama"
```

Fallback behavior:
- If primary returns HTTP 5xx or times out, try the fallback
- If primary returns HTTP 429 (rate limit), try the fallback
- If primary returns HTTP 4xx (client error), do NOT fall back (the request itself is bad)
- Fallback chains can have multiple levels (primary -> secondary -> tertiary)

### 6.9 Budget Alerts and Limits

MLflow 3.9+ added budget management to the Gateway:

```yaml
# Budget configuration (per endpoint)
endpoints:
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}
    budget:
      max_daily_cost_usd: 50.00
      alert_threshold_pct: 80    # Alert at 80% of budget
      action_on_exceed: block     # "block" or "alert"
```

Budget tracking uses the same token-to-cost computation as MLflow Tracing. When the budget threshold is hit, the Gateway can either:
- **Alert**: Log a warning and continue serving (for monitoring)
- **Block**: Return HTTP 429 and stop serving requests (for hard limits)

### 6.10 Calling the Gateway from Agent Code

There are two ways to route agent traffic through the Gateway:

**Option A: Use the MLflow Deployments SDK**

```python
from mlflow.deployments import get_deploy_client

client = get_deploy_client("http://localhost:5000")

response = client.predict(
    endpoint="chat",
    inputs={
        "messages": [
            {"role": "user", "content": "Check health of all bronze tables"}
        ],
        "max_tokens": 4096,
    },
)
```

**Option B: Use the provider SDK with a custom base URL**

```python
# Point the Anthropic/OpenAI client at the Gateway
import openai

client = openai.OpenAI(
    base_url="http://localhost:5000/gateway/chat",
    api_key="not-needed-gateway-handles-auth",
)

response = client.chat.completions.create(
    model="claude-sonnet-4-5-20250514",
    messages=[{"role": "user", "content": "Hello"}],
)
```

Option B works because the Gateway speaks the OpenAI ChatCompletion wire format, regardless of the underlying provider.

Source: [MLflow AI Gateway — Querying](https://mlflow.org/docs/latest/llms/gateway/index.html#querying-the-gateway)

---

## 7. Unity Catalog Model Registry Integration

### 7.1 The Idea: Governed Model Lifecycle

Unity Catalog (UC) provides a three-level namespace for data assets: `catalog.schema.table`. MLflow can use UC as its model registry backend, giving models the same governance as tables:

```
unity.models.pipeline_guardian        # A model in the "models" schema
unity.models.data_analyst             # Another model
unity.models.pipeline_autopilot       # Another model
iceberg.bronze.orders                 # A table (same namespace structure)
```

This means:
- Models and tables live in the same catalog
- Access control is unified (UC permissions apply to both)
- Lineage is traceable (which model reads which table)
- Model versions are managed alongside data versions

### 7.2 Setting the Registry URI

To use Unity Catalog as the MLflow model registry:

```python
import mlflow

# Point the registry at Unity Catalog
mlflow.set_registry_uri("uc:http://localhost:8080")

# Now model registration goes to UC instead of the MLflow backend
with mlflow.start_run():
    mlflow.pyfunc.log_model(
        artifact_path="guardian_agent",
        python_model=agent,
        registered_model_name="unity.models.pipeline_guardian",  # Three-level namespace
    )
```

The `uc:` prefix tells MLflow to use the Unity Catalog REST API. The URL is the UC server endpoint (port 8080 in the lakehouse stack).

### 7.3 Three-Level Namespace

UC model names follow the `catalog.schema.model` convention:

```python
# Register a model
mlflow.pyfunc.log_model(
    artifact_path="agent",
    python_model=agent,
    registered_model_name="unity.models.pipeline_guardian",
    #                       ^^^^^^^  ^^^^^^  ^^^^^^^^^^^^^^^^
    #                       catalog  schema  model name
)

# Load a specific version
model = mlflow.pyfunc.load_model("models:/unity.models.pipeline_guardian/3")

# Load the latest version with an alias
model = mlflow.pyfunc.load_model("models:/unity.models.pipeline_guardian@production")
```

Model versions in UC support:
- **Version numbers**: Auto-incrementing (1, 2, 3, ...)
- **Aliases**: Named references ("production", "staging", "champion", "challenger")
- **Tags**: Key-value metadata per version
- **Descriptions**: Human-readable notes

### 7.4 OSS UC + MLflow = Governed Model Lifecycle

When you combine OSS Unity Catalog (0.3.1+) with MLflow 3.x, you get a fully open-source governed model lifecycle:

```
Developer machine                    Unity Catalog (localhost:8080)
┌────────────────────┐              ┌──────────────────────────────┐
│  MLflow Agent Code │              │  unity catalog               │
│                    │  register    │  ├── models schema           │
│  agent = Guardian()│ ──────────→  │  │   ├── pipeline_guardian   │
│  mlflow.log_model()│              │  │   │   ├── v1 (staging)    │
│                    │              │  │   │   └── v2 (production) │
│                    │  load        │  │   ├── data_analyst        │
│  mlflow.load_model │ ←──────────  │  │   └── pipeline_autopilot │
│                    │              │  ├── bronze schema            │
│                    │              │  │   ├── orders (table)       │
│                    │              │  │   └── dim_locations        │
└────────────────────┘              └──────────────────────────────┘
```

Important caveat: OSS Unity Catalog provides the catalog and registry. It does **not** provide:
- Automated model deployment (you deploy manually or via CI/CD)
- A/B testing infrastructure
- Production model monitoring
- Auto-scaling endpoints

Those capabilities exist in Databricks Unity Catalog. See [Section 10](#10-oss-vs-databricks-mlflow) for the full comparison.

Source:
- [MLflow Unity Catalog Integration](https://mlflow.org/docs/latest/plugins.html#unity-catalog)
- [Unity Catalog OSS — MLflow Integration](https://docs.unitycatalog.io/integrations/unity-catalog-mlflow/)

---

## 8. The Three Agent Patterns

Act 6 of the OverArchitected show demonstrates three increasingly autonomous agent patterns, all built on the same MLflow infrastructure. Each pattern addresses a real operational need for data teams managing a lakehouse.

### 8.1 Option A: Pipeline Guardian (06a)

**Purpose**: Table health monitoring and maintenance execution.

**Architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     PIPELINE GUARDIAN                             │
│                                                                  │
│  User: "Check health of all bronze tables"                       │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────────────────────┐                                   │
│  │  PipelineGuardianAgent   │   MLflow ChatAgent                │
│  │  (predict method)        │   @mlflow.trace decorated         │
│  └──────────┬───────────────┘                                   │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────────┐   ┌──────────────────────────┐   │
│  │  LLM (Claude / Ollama)  │──▶│  Tool Selection           │   │
│  │  via Anthropic SDK or   │   │  - inspect_table          │   │
│  │  OpenAI-compatible API  │   │  - check_quality          │   │
│  └──────────────────────────┘   │  - run_maintenance        │   │
│                                  │  - query_table            │   │
│                                  └───────────┬──────────────┘   │
│                                              │                   │
│                                              ▼                   │
│                                  ┌──────────────────────────┐   │
│                                  │  Spark SQL Execution      │   │
│                                  │  (local[*] or Connect)    │   │
│                                  └───────────┬──────────────┘   │
│                                              │                   │
│                                              ▼                   │
│                                  ┌──────────────────────────┐   │
│                                  │  Iceberg Tables           │   │
│                                  │  .snapshots, .files,      │   │
│                                  │  system.rewrite_data_files│   │
│                                  └──────────────────────────┘   │
│                                                                  │
│  Every LLM call + tool call = MLflow Trace span                  │
└─────────────────────────────────────────────────────────────────┘
```

**Tools**:

| Tool | Input | Output | Spark SQL Used |
|------|-------|--------|----------------|
| `inspect_table` | `table_name` | Row count, snapshot count, file count, total size | `SELECT COUNT(*)`, `{table}.snapshots`, `{table}.files` |
| `check_quality` | `table_name` | Null rates per column, data freshness, duplicate count | `IS NULL` filters, `MAX(ts)`, `DISTINCT` count |
| `run_maintenance` | `table_name`, `action` | Status and detail of action taken | `CALL iceberg.system.rewrite_data_files()`, `expire_snapshots()`, `remove_orphan_files()` |
| `query_table` | `sql` | Columns, row count, up to 20 rows of data | Arbitrary SQL |

**System Prompt** (abbreviated):

> You are the Pipeline Guardian -- an AI agent that monitors and maintains a data lakehouse built on Apache Spark + Iceberg + PostgreSQL + SeaweedFS.
>
> Your responsibilities:
> 1. Inspect table health (row counts, snapshots, file counts)
> 2. Check data quality (null rates, freshness, duplicates)
> 3. Recommend and execute maintenance (compaction, snapshot expiry, orphan cleanup)
> 4. Answer questions about the data by running SQL queries

**Interaction Flow**:

1. User asks: "Check health of all bronze tables"
2. LLM decides to call `inspect_table` for each bronze table (5 calls)
3. LLM receives results, identifies issues (e.g., high file count on orders table)
4. LLM recommends compaction, optionally calls `run_maintenance`
5. LLM summarizes findings in natural language

**Key Design Decisions**:
- Tools are pure functions with no side effects except `run_maintenance`
- Maximum 10 tool-calling iterations (prevents runaway loops)
- Both Anthropic and OpenAI tool-calling formats supported
- Agent is logged to MLflow model registry after demo

### 8.2 Option B: Data Quality Analyst (06b)

**Purpose**: Natural language to Spark SQL. Ask questions about your data in English.

**Architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA QUALITY ANALYST                          │
│                                                                  │
│  User: "What are the peak ordering hours?"                       │
│       │                                                          │
│       ▼                                                          │
│  ┌──────────────────────────┐                                   │
│  │  DataAnalystAgent        │   MLflow ChatAgent                │
│  │  (predict method)        │                                    │
│  └──────────┬───────────────┘                                   │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────────┐                                   │
│  │  LLM + Schema Context   │   System prompt includes:          │
│  │                          │   - All table schemas              │
│  │  "Here are the tables,  │   - Column names and types         │
│  │   columns, types, and   │   - Row counts                     │
│  │   row counts..."        │   - Data notes (JSON extraction,   │
│  │                          │     timestamp format, event types) │
│  └──────────┬───────────────┘                                   │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────────┐                                   │
│  │  Tool: query_lakehouse   │   Single tool, unbounded SQL      │
│  │  - Executes any SQL      │   Up to 50 rows returned          │
│  │  - Returns columns +     │   Up to 8 tool-calling iterations │
│  │    data as JSON          │                                    │
│  └───────────┬──────────────┘                                   │
│              │                                                   │
│              ▼                                                   │
│  ┌──────────────────────────┐                                   │
│  │  Spark SQL Engine        │                                   │
│  │  (Iceberg tables)        │                                   │
│  └──────────────────────────┘                                   │
│                                                                  │
│  LLM generates SQL → executes → formats answer in English        │
└─────────────────────────────────────────────────────────────────┘
```

**Key Difference from Guardian**: The Analyst has only one tool (`query_lakehouse`) but compensates with a rich schema context injected into the system prompt. This is the **text-to-SQL pattern** -- the LLM generates SQL based on schema knowledge, executes it, and interprets the results.

**Schema Context Generation**:

The `get_schema_context()` function dynamically reads table schemas at startup:

```python
def get_schema_context():
    """Build schema context string for the LLM."""
    spark = get_spark()
    schemas = {}
    for table in tables:
        df = spark.table(table)
        cols = [(c.name, c.dataType.simpleString()) for c in df.schema]
        count = df.count()
        schemas[table] = {"columns": cols, "row_count": count}
    # Format into a human-readable string
```

The generated context looks like:

```
iceberg.bronze.orders (14,283 rows):
  - order_id: string
  - ts: string
  - event_type: string
  - sequence: int
  - body: string
  - city: string

Notes on the data:
- orders.ts is ISO 8601 string. Use TO_TIMESTAMP(REPLACE(ts, 'T', ' ')) to convert.
- orders.body is a JSON string. Use get_json_object(body, '$.field') to extract fields.
- orders.event_type values: order_created, kitchen_started, ...
```

**Demo Queries**:

| Question | LLM-Generated SQL (typical) |
|----------|---------------------------|
| "Total order volume by city?" | `SELECT city, COUNT(*) as orders FROM iceberg.bronze.orders WHERE event_type = 'order_created' GROUP BY city ORDER BY orders DESC` |
| "Peak ordering hours?" | `SELECT HOUR(TO_TIMESTAMP(REPLACE(ts, 'T', ' '))) as hour, COUNT(*) FROM iceberg.bronze.orders WHERE event_type = 'order_created' GROUP BY 1 ORDER BY 1` |
| "Top 5 brands by average order value?" | `SELECT b.name, AVG(CAST(get_json_object(o.body, '$.total') AS DOUBLE)) as avg_total FROM iceberg.bronze.orders o JOIN iceberg.bronze.dim_brands b ON get_json_object(o.body, '$.brand_id') = b.id WHERE o.event_type = 'order_created' GROUP BY b.name ORDER BY avg_total DESC LIMIT 5` |

### 8.3 Option C: Pipeline Autopilot (06c)

**Purpose**: Autonomous monitoring loop that detects anomalies and takes corrective action without human intervention.

**Architecture**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     PIPELINE AUTOPILOT                            │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                  MONITORING LOOP                           │   │
│  │  Cycle 1 → Cycle 2 → Cycle 3 → ... (every 60s)          │   │
│  └───────────────────────┬──────────────────────────────────┘   │
│                          │                                       │
│                          ▼                                       │
│  ┌──────────────────────────┐                                   │
│  │  PipelineAutopilotAgent  │   MLflow ChatAgent                │
│  │  (12 max iterations)     │   Highest autonomy level          │
│  └──────────┬───────────────┘                                   │
│             │                                                    │
│             ▼                                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    TOOL SUITE                              │   │
│  │                                                            │   │
│  │  collect_metrics          execute_maintenance              │   │
│  │  ┌────────────────────┐  ┌────────────────────────┐      │   │
│  │  │ All tables:        │  │ compact                 │      │   │
│  │  │ - row_count        │  │ expire_snapshots        │      │   │
│  │  │ - null_pct per col │  │ remove_orphans          │      │   │
│  │  │ - file_count       │  └────────────────────────┘      │   │
│  │  │ - snapshot_count   │                                   │   │
│  │  │ - anomaly detection│  check_streaming                  │   │
│  │  └────────────────────┘  ┌────────────────────────┐      │   │
│  │                          │ Kafka connectivity      │      │   │
│  │                          │ Active streaming queries│      │   │
│  │                          └────────────────────────┘      │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│  THRESHOLDS (configurable):                                      │
│  - max_null_rate_pct: 10%    - max_file_count: 500              │
│  - max_stale_hours: 24       - min_row_count: 100               │
│  - max_snapshot_count: 20                                        │
│                                                                  │
│  DECISION FLOW:                                                  │
│  collect_metrics → anomalies detected? → execute_maintenance     │
│                 → streaming health? → report findings             │
└─────────────────────────────────────────────────────────────────┘
```

**What Makes This Different**:

| Aspect | Guardian (6a) | Analyst (6b) | Autopilot (6c) |
|--------|---------------|--------------|-----------------|
| Trigger | Human query | Human question | Timer / monitoring loop |
| Autonomy | Responds to requests | Responds to requests | Proactively acts |
| Tool count | 4 | 1 | 3 |
| Max iterations | 10 | 8 | 12 |
| Writes data | Yes (maintenance) | No (read-only) | Yes (maintenance) |
| Monitoring loop | No | No | Yes (`--monitor` flag) |

**Anomaly Detection**:

The `collect_pipeline_metrics()` function checks every table against configured thresholds:

```python
THRESHOLDS = {
    "max_null_rate_pct": 10.0,     # Alert if any column > 10% null
    "max_stale_hours": 24,         # Alert if no data in 24h
    "max_file_count": 500,         # Recommend compaction above 500 files
    "min_row_count": 100,          # Alert if table has < 100 rows
    "max_snapshot_count": 20,      # Recommend expiry above 20 snapshots
}
```

Anomalies are returned with severity levels:
- **WARNING**: Something to watch (low row count, high null rate)
- **ACTION_NEEDED**: Automated maintenance recommended (high file count, too many snapshots)

**Monitoring Loop**:

```bash
# Run in continuous monitoring mode
python scripts/demos/overarchitected/06c_mlflow_autopilot.py --monitor --interval 30

# This runs up to 10 cycles, each cycle:
# 1. Agent collects metrics from all tables
# 2. Agent analyzes anomalies
# 3. Agent executes maintenance if needed
# 4. Agent reports findings
# 5. Sleep for interval seconds
# 6. Repeat
```

Each monitoring cycle is a separate MLflow run, creating a time series of health checks in the experiment.

**Streaming Health Check**:

The `check_streaming_health()` function tests Kafka connectivity and active Spark streaming queries:

```python
def check_streaming_health() -> dict:
    # TCP connect to Kafka (localhost:9092)
    # List active Spark streaming queries
    # Return status of both
```

This bridges the Autopilot into the streaming infrastructure, making it a true full-stack monitor.

### 8.4 Common Patterns Across All Three Agents

Despite their different purposes, all three agents share these patterns:

**1. Provider Abstraction**:
Every agent supports both Anthropic and OpenAI-compatible APIs through a `LLM_PROVIDER` environment variable:

```python
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic")  # or "openai" for Ollama
```

**2. Tool-Calling Loop**:
All agents implement the same loop: call LLM -> check for tool calls -> execute tools -> feed results back -> repeat until LLM stops calling tools.

**3. Trace Everything**:
Every agent decorates `predict()` with `@mlflow.trace` and wraps tool calls in `mlflow.start_span()`.

**4. Model Registration**:
Every agent logs itself to the MLflow model registry after the demo, making it servable:

```python
mlflow.pyfunc.log_model(
    artifact_path="guardian_agent",
    python_model=agent,
    registered_model_name="pipeline-guardian",
)
```

**5. Spark Connection Flexibility**:
Every agent supports both local Spark and Spark Connect:

```python
SPARK_REMOTE = os.getenv("SPARK_REMOTE", None)  # sc://host:15002
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

def get_spark():
    if SPARK_REMOTE:
        return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    return SparkSession.builder.master(SPARK_MASTER).getOrCreate()
```

---

## 9. Ollama as OSS LLM Fallback

### 9.1 Why Ollama

The demo agents default to Anthropic Claude, which requires an API key and incurs cost. For local development, testing, and fully-offline operation, [Ollama](https://ollama.com/) provides:

- Local LLM inference with no API keys
- An OpenAI-compatible API at `http://localhost:11434/v1`
- A growing library of open-weight models
- Single-binary installation on Linux, macOS, and Windows

This makes Ollama the natural fallback for the MLflow AI Gateway.

### 9.2 Installation

```bash
# Linux / macOS
curl -fsSL https://ollama.com/install.sh | sh

# macOS (Homebrew)
brew install ollama

# Start the server (if not auto-started)
ollama serve

# Verify
curl http://localhost:11434/api/version
```

Source: [Ollama Installation](https://ollama.com/download)

### 9.3 Model Selection: qwen2.5:14b for Tool Calling

Not all open models support tool calling (function calling). For the demo agents to work, the model must:

1. Support the OpenAI tool-calling format
2. Reliably generate valid JSON for tool arguments
3. Know when to stop calling tools and generate a text response

Recommended models for agentic use cases (as of early 2026):

| Model | Size | Tool Calling | Notes |
|-------|------|-------------|-------|
| **qwen2.5:14b** | 8.5 GB | Excellent | Best balance of quality and speed for tool calling |
| qwen2.5:7b | 4.4 GB | Good | Faster but less reliable on complex tool schemas |
| qwen2.5:32b | 19 GB | Excellent | Higher quality, needs 24+ GB VRAM |
| llama3.3:70b | 40 GB | Good | Highest quality, needs 48+ GB VRAM |
| mistral-small:22b | 13 GB | Good | Strong on structured output |
| deepseek-r1:14b | 9 GB | Fair | Reasoning model, slower tool calling |

The demo uses `qwen2.5:14b` as the default:

```bash
# Pull the model (8.5 GB download)
ollama pull qwen2.5:14b

# Test tool calling
curl http://localhost:11434/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "qwen2.5:14b",
    "messages": [{"role": "user", "content": "What is 2+2?"}],
    "tools": [{
      "type": "function",
      "function": {
        "name": "calculate",
        "description": "Perform arithmetic",
        "parameters": {"type": "object", "properties": {"expression": {"type": "string"}}, "required": ["expression"]}
      }
    }]
  }'
```

Source: [Ollama Model Library](https://ollama.com/library)

### 9.4 OpenAI-Compatible API

Ollama exposes an OpenAI-compatible API at `/v1/chat/completions`, which means any code written for the OpenAI SDK works with Ollama by changing the base URL:

```python
import openai

# This is all you need to switch from OpenAI to Ollama
client = openai.OpenAI(
    base_url="http://localhost:11434/v1",
    api_key="ollama",  # Required by the SDK, but Ollama ignores it
)

response = client.chat.completions.create(
    model="qwen2.5:14b",
    messages=[{"role": "user", "content": "Hello"}],
    tools=[...],  # Tool calling works the same way
)
```

Supported OpenAI-compatible endpoints:

| Endpoint | Status | Notes |
|----------|--------|-------|
| `/v1/chat/completions` | Supported | Including tool calling and streaming |
| `/v1/completions` | Supported | Text completion |
| `/v1/embeddings` | Supported | With embedding models |
| `/v1/models` | Supported | List available models |

Source: [Ollama OpenAI Compatibility](https://ollama.com/blog/openai-compatibility)

### 9.5 Gateway Configuration for Fallback Routing

The MLflow AI Gateway config routes to Ollama as a fallback:

```yaml
# config/mlflow/gateway-config.yml
endpoints:
  # Primary: Anthropic Claude
  - name: chat
    endpoint_type: llm/v1/chat
    model:
      provider: anthropic
      name: claude-sonnet-4-5-20250514
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}

  # Fallback: Ollama (local)
  - name: chat-local
    endpoint_type: llm/v1/chat
    model:
      provider: openai
      name: qwen2.5:14b
      config:
        openai_api_base: http://localhost:11434/v1
        openai_api_key: "ollama"
```

To use Ollama directly (bypassing the Gateway), set environment variables:

```bash
export LLM_PROVIDER=openai
export LLM_MODEL=qwen2.5:14b
export OPENAI_BASE_URL=http://localhost:11434/v1

python scripts/demos/overarchitected/06a_mlflow_guardian.py
```

### 9.6 Performance Considerations

| Factor | Anthropic Claude | Ollama qwen2.5:14b |
|--------|------------------|---------------------|
| Latency (first token) | 0.5-2s | 1-5s (depends on hardware) |
| Throughput | ~80 tokens/s | ~15-40 tokens/s (GPU dependent) |
| Tool calling reliability | 99%+ | ~90-95% |
| Cost | ~$3-15/1M input tokens | $0 (hardware costs only) |
| Privacy | Data sent to API | Fully local |
| Availability | Requires internet | Works offline |
| VRAM needed | N/A | 10-12 GB for 14B model |

For the demo, Anthropic Claude is preferred for reliability. Ollama is the fallback for offline environments, cost-sensitive development, or data-sovereignty requirements.

---

## 10. OSS vs Databricks MLflow

### 10.1 What Is Fully Open Source

The following MLflow capabilities are **100% open source** (Apache 2.0 license) and run entirely on your infrastructure:

| Capability | OSS Status | Notes |
|------------|-----------|-------|
| **Experiment Tracking** | Full | Runs, metrics, parameters, artifacts, tags |
| **MLflow Tracing** | Full | OTel-compatible, auto-tracing for 40+ libraries |
| **AI Gateway** | Full | Embedded in tracking server, YAML-configured |
| **Agent Server** | Full | FastAPI-based, streaming, Docker packaging |
| **ChatAgent / ResponsesAgent** | Full | Framework-agnostic agent interfaces |
| **Model Registry** | Full | Versioning, aliases, tags, descriptions |
| **Unity Catalog Integration** | Full | OSS UC as registry backend |
| **Model Evaluation** | Full | `mlflow.evaluate()` with GenAI metrics |
| **LLM-as-Judge** | Full | Automated evaluation using LLMs |
| **Prompt Engineering UI** | Full | Built into tracking UI |
| **Docker Packaging** | Full | `mlflow models build-docker` |
| **REST API** | Full | All tracking/registry operations |
| **Python/R/Java/REST Clients** | Full | Multi-language SDKs |

Source: [MLflow GitHub Repository (Apache 2.0)](https://github.com/mlflow/mlflow/blob/master/LICENSE.txt)

### 10.2 What Requires Databricks

The following capabilities are **only available on Databricks** (as of early 2026):

| Capability | What It Provides | OSS Alternative |
|------------|-----------------|-----------------|
| **Mosaic AI Agent Framework** | Managed agent deployment with auto-scaling | Self-hosted Agent Server + Kubernetes |
| **Agent Evaluation (Review App)** | Human-in-the-loop review UI for agent responses | Manual testing or custom UI |
| **Production Model Monitoring** | Drift detection, data quality monitoring at scale | Prometheus + custom dashboards |
| **Auto-scaling Endpoints** | Serverless model serving with auto-scaling | Kubernetes HPA + Agent Server containers |
| **Feature Serving** | Real-time feature lookup for models | Feature stores (Feast, etc.) |
| **Managed MLflow** | Zero-ops tracking server | Self-hosted MLflow (as in our docker-compose) |
| **Lakehouse Monitoring** | Table-level data quality monitoring | Custom agents (like our Autopilot) |
| **Vector Search** | Managed vector database for RAG | Self-hosted Milvus, Qdrant, pgvector |
| **Playground** | Interactive LLM testing UI | MLflow Prompt Engineering UI (more limited) |
| **Genie** | Natural language data querying | Custom agents (like our Analyst) |

### 10.3 The Honest Assessment

For this project (self-hosted lakehouse), the OSS MLflow stack provides:

**What works well:**
- Tracking, tracing, and the AI Gateway are production-grade
- Agent interfaces are clean and framework-agnostic
- Unity Catalog integration provides real governance
- Docker packaging enables standard deployment workflows

**Where you feel the gap:**
- **No managed serving**: You must operate your own infrastructure (Kubernetes, load balancers, health checks)
- **No Review App**: Evaluating agent quality requires custom tooling or manual testing
- **No production monitoring**: You need to build drift detection and alerting yourself
- **Limited evaluation**: OSS `mlflow.evaluate()` works but lacks the depth of Databricks' agent evaluation suite

**Bottom line**: The OSS stack is sufficient for development, prototyping, and small-scale production. For enterprise-scale agent deployment with SLAs, the Databricks managed layer adds real value. This is the classic open-core trade-off.

---

## 11. Practical Setup

### 11.1 docker-compose-mlflow.yml Explained

The MLflow service runs as a single container:

```yaml
# docker-compose-mlflow.yml
services:
  mlflow:
    image: ghcr.io/mlflow/mlflow:v3.1.0    # Official MLflow container
    container_name: mlflow-server
    ports:
      - "5000:5000"                          # Tracking UI + Gateway
    environment:
      # Backend: PostgreSQL (shared with lakehouse metadata)
      MLFLOW_BACKEND_STORE_URI: "postgresql://mlflow:mlflow_password@localhost:5432/mlflow"

      # Artifacts: SeaweedFS via S3 protocol
      MLFLOW_ARTIFACTS_DESTINATION: "s3://lakehouse/mlflow-artifacts"
      AWS_ACCESS_KEY_ID: "${S3_ACCESS_KEY:-admin}"
      AWS_SECRET_ACCESS_KEY: "${S3_SECRET_KEY:-admin_password}"
      MLFLOW_S3_ENDPOINT_URL: "http://localhost:8333"

      # AI Gateway config file
      MLFLOW_GATEWAY_CONFIG: "/config/gateway-config.yml"
    volumes:
      - ./config/mlflow:/config:ro            # Gateway config (read-only)
      - mlflow-data:/mlflow                   # Local data volume
    network_mode: host                        # Share host network (access PG, SeaweedFS, Ollama)
    command: >
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri postgresql://mlflow:mlflow_password@localhost:5432/mlflow
      --default-artifact-root s3://lakehouse/mlflow-artifacts
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 15s
```

Key design decisions:

| Decision | Rationale |
|----------|-----------|
| `network_mode: host` | MLflow needs to reach PostgreSQL (5432), SeaweedFS (8333), and Ollama (11434) on localhost |
| PostgreSQL backend | Same database server as lakehouse metadata -- no new infrastructure |
| SeaweedFS artifacts | Same S3-compatible storage as Iceberg data -- no new infrastructure |
| Single container | Tracking server + Gateway in one process -- simpler operations |

### 11.2 gateway-config.yml Explained

```yaml
# config/mlflow/gateway-config.yml
endpoints:
  - name: chat                              # Endpoint name (used in URL)
    endpoint_type: llm/v1/chat              # Chat completion format
    model:
      provider: anthropic                    # Use Anthropic's API
      name: claude-sonnet-4-5-20250514                # Model name
      config:
        anthropic_api_key: ${ANTHROPIC_API_KEY}  # From environment variable

  - name: chat-local                        # Ollama fallback
    endpoint_type: llm/v1/chat
    model:
      provider: openai                       # Ollama speaks OpenAI format
      name: qwen2.5:14b
      config:
        openai_api_base: http://localhost:11434/v1
        openai_api_key: "ollama"             # Dummy key (Ollama ignores it)
```

The `${ANTHROPIC_API_KEY}` syntax means the Gateway reads the value from the environment variable at runtime. Set it before starting the container:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
docker compose -f docker-compose-mlflow.yml up -d
```

### 11.3 Environment Variables

Complete list of environment variables used by the Act 6 demo:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MLFLOW_TRACKING_URI` | `http://localhost:5000` | MLflow tracking server URL |
| `ANTHROPIC_API_KEY` | (none) | Anthropic API key for Claude |
| `LLM_PROVIDER` | `anthropic` | `anthropic` or `openai` (for Ollama) |
| `LLM_MODEL` | `claude-sonnet-4-5-20250514` | Model name to use |
| `OPENAI_BASE_URL` | `http://localhost:11434/v1` | Base URL for OpenAI-compatible API (Ollama) |
| `SPARK_REMOTE` | (none) | Spark Connect URL (e.g., `sc://localhost:15002`) |
| `SPARK_MASTER` | `local[*]` | Spark master URL (if not using Connect) |
| `S3_ACCESS_KEY` | `admin` | SeaweedFS / S3 access key |
| `S3_SECRET_KEY` | `admin_password` | SeaweedFS / S3 secret key |

### 11.4 Connecting Agents to Spark via Spark Connect

The demo agents can connect to Spark in two ways:

**Option 1: Local Spark (default)**

```bash
# Agent runs PySpark locally (requires JVM on the machine)
export SPARK_MASTER=local[*]
python scripts/demos/overarchitected/06a_mlflow_guardian.py
```

This is simple but requires Java and the full PySpark installation on the machine running the agent.

**Option 2: Spark Connect (recommended for production)**

```bash
# Start Connect server on the Spark cluster
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078

# Agent connects as thin client (no JVM needed)
pip install pyspark-client  # Lightweight client, no JVM
export SPARK_REMOTE=sc://localhost:15002
python scripts/demos/overarchitected/06a_mlflow_guardian.py
```

With Spark Connect, the agent machine only needs `pyspark-client` (a few MB) instead of the full PySpark installation (hundreds of MB). This is how you would deploy agents in production: the agent container is lightweight, and Spark execution happens on the cluster.

```
┌─────────────────────┐         ┌─────────────────────────────┐
│  Agent Container     │         │  Spark Cluster               │
│  ┌───────────────┐  │  gRPC   │  ┌─────────────────────────┐│
│  │ Guardian Agent │  │────────▶│  │ Spark Connect Server    ││
│  │ (Python only)  │  │         │  │ (port 15002)            ││
│  │ pyspark-client │  │         │  └───────────┬─────────────┘│
│  └───────────────┘  │         │              │               │
│  No JVM needed      │         │  ┌───────────▼─────────────┐│
│                     │         │  │ Spark Workers            ││
│                     │         │  │ (Iceberg, SQL execution) ││
└─────────────────────┘         │  └─────────────────────────┘│
                                └─────────────────────────────┘
```

Source: [Spark Connect documentation](https://spark.apache.org/docs/latest/spark-connect-overview.html)

### 11.5 Full Setup Walkthrough

Here is the complete sequence to go from zero to running agents:

```bash
# 1. Start the lakehouse stack
./lakehouse start all

# 2. Generate and load test data
./lakehouse testdata generate --days 7
./lakehouse testdata load

# 3. (Optional) Start Unity Catalog for model registry
./lakehouse start unity-catalog

# 4. Create MLflow database in PostgreSQL
docker exec -it postgres psql -U postgres -c "CREATE DATABASE mlflow;"
docker exec -it postgres psql -U postgres -c "CREATE USER mlflow WITH PASSWORD 'mlflow_password';"
docker exec -it postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE mlflow TO mlflow;"

# 5. Start MLflow tracking server + AI Gateway
export ANTHROPIC_API_KEY=sk-ant-...
docker compose -f docker-compose-mlflow.yml up -d

# 6. Verify MLflow is running
curl http://localhost:5000/health
# Open http://localhost:5000 in browser

# 7. Install Python dependencies
pip install mlflow>=3.1 anthropic openai pyspark

# 8. (Optional) Install Ollama for local LLM fallback
curl -fsSL https://ollama.com/install.sh | sh
ollama pull qwen2.5:14b

# 9. Run the agents
python scripts/demos/overarchitected/06a_mlflow_guardian.py
python scripts/demos/overarchitected/06b_mlflow_analyst.py
python scripts/demos/overarchitected/06c_mlflow_autopilot.py

# 10. (Optional) Run with Ollama instead of Anthropic
export LLM_PROVIDER=openai
export LLM_MODEL=qwen2.5:14b
export OPENAI_BASE_URL=http://localhost:11434/v1
python scripts/demos/overarchitected/06a_mlflow_guardian.py

# 11. (Optional) Start Spark Connect and run agents as thin clients
docker exec spark-master-41 /opt/spark/sbin/start-connect-server.sh \
    --master spark://spark-master-41:7078
export SPARK_REMOTE=sc://localhost:15002
python scripts/demos/overarchitected/06a_mlflow_guardian.py

# 12. View traces in MLflow UI
# Open http://localhost:5000
# Navigate to Experiments → overarchitected-guardian → Traces
```

### 11.6 Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `ConnectionError: localhost:5000` | MLflow not running | `docker compose -f docker-compose-mlflow.yml up -d` |
| `AuthenticationError` (Anthropic) | Missing or invalid API key | `export ANTHROPIC_API_KEY=sk-ant-...` |
| `ConnectionError: localhost:11434` | Ollama not running | `ollama serve` |
| `Model not found: qwen2.5:14b` | Model not pulled | `ollama pull qwen2.5:14b` |
| `AnalysisException: Table not found` | Test data not loaded | `./lakehouse testdata generate --days 7 && ./lakehouse testdata load` |
| `ConnectionError: localhost:15002` | Spark Connect not started | Start Connect server (see 11.4) |
| `psycopg2.OperationalError` | MLflow database not created | Create the `mlflow` database in PostgreSQL (see step 4) |
| Traces not appearing in UI | Auto-logging not enabled | Add `mlflow.anthropic.autolog()` or `mlflow.openai.autolog()` |
| Gateway returns 404 | Endpoint name mismatch | Check `gateway-config.yml` endpoint names match your request |
| OOM on Ollama | Model too large for available RAM/VRAM | Use smaller model (`qwen2.5:7b`) or increase memory |

---

## 12. References

### Official Documentation

| Resource | URL |
|----------|-----|
| MLflow Documentation | [https://mlflow.org/docs/latest/](https://mlflow.org/docs/latest/) |
| MLflow Tracing | [https://mlflow.org/docs/latest/llms/tracing/index.html](https://mlflow.org/docs/latest/llms/tracing/index.html) |
| MLflow ChatAgent | [https://mlflow.org/docs/latest/llms/chat-agent/index.html](https://mlflow.org/docs/latest/llms/chat-agent/index.html) |
| MLflow ResponsesAgent | [https://mlflow.org/docs/latest/llms/responses-agent/index.html](https://mlflow.org/docs/latest/llms/responses-agent/index.html) |
| MLflow AI Gateway | [https://mlflow.org/docs/latest/llms/gateway/index.html](https://mlflow.org/docs/latest/llms/gateway/index.html) |
| MLflow Model Deployment | [https://mlflow.org/docs/latest/deployment/index.html](https://mlflow.org/docs/latest/deployment/index.html) |
| MLflow Model Registry | [https://mlflow.org/docs/latest/model-registry.html](https://mlflow.org/docs/latest/model-registry.html) |
| MLflow Unity Catalog Plugin | [https://mlflow.org/docs/latest/plugins.html#unity-catalog](https://mlflow.org/docs/latest/plugins.html#unity-catalog) |
| MLflow Python API Reference | [https://mlflow.org/docs/latest/python_api/index.html](https://mlflow.org/docs/latest/python_api/index.html) |

### Source Code

| Resource | URL |
|----------|-----|
| MLflow GitHub | [https://github.com/mlflow/mlflow](https://github.com/mlflow/mlflow) |
| MLflow Changelog | [https://github.com/mlflow/mlflow/blob/master/CHANGELOG.md](https://github.com/mlflow/mlflow/blob/master/CHANGELOG.md) |
| MLflow Docker Images | [https://github.com/mlflow/mlflow/pkgs/container/mlflow](https://github.com/mlflow/mlflow/pkgs/container/mlflow) |
| Unity Catalog OSS | [https://github.com/unitycatalog/unitycatalog](https://github.com/unitycatalog/unitycatalog) |
| UC MLflow Integration | [https://docs.unitycatalog.io/integrations/unity-catalog-mlflow/](https://docs.unitycatalog.io/integrations/unity-catalog-mlflow/) |

### Ollama

| Resource | URL |
|----------|-----|
| Ollama | [https://ollama.com/](https://ollama.com/) |
| Ollama Model Library | [https://ollama.com/library](https://ollama.com/library) |
| Ollama OpenAI Compatibility | [https://ollama.com/blog/openai-compatibility](https://ollama.com/blog/openai-compatibility) |
| Ollama GitHub | [https://github.com/ollama/ollama](https://github.com/ollama/ollama) |

### Lakehouse Stack (This Project)

| Resource | Path |
|----------|------|
| Demo: Pipeline Guardian | `scripts/demos/overarchitected/06a_mlflow_guardian.py` |
| Demo: Data Analyst | `scripts/demos/overarchitected/06b_mlflow_analyst.py` |
| Demo: Pipeline Autopilot | `scripts/demos/overarchitected/06c_mlflow_autopilot.py` |
| Docker Compose (MLflow) | `docker-compose-mlflow.yml` |
| Gateway Config | `config/mlflow/gateway-config.yml` |
| Show README | `scripts/demos/overarchitected/README.md` |

### Blog Posts and Talks

| Resource | URL |
|----------|-----|
| MLflow 3.0 Announcement | [https://mlflow.org/blog/mlflow-3-0](https://mlflow.org/blog/mlflow-3-0) |
| MLflow for GenAI Overview | [https://mlflow.org/docs/latest/llms/index.html](https://mlflow.org/docs/latest/llms/index.html) |
| OpenTelemetry Specification | [https://opentelemetry.io/docs/specs/otel/](https://opentelemetry.io/docs/specs/otel/) |
| Spark Connect Overview | [https://spark.apache.org/docs/latest/spark-connect-overview.html](https://spark.apache.org/docs/latest/spark-connect-overview.html) |
| PyPI MLflow Statistics | [https://pypistats.org/packages/mlflow](https://pypistats.org/packages/mlflow) |

---

*This companion guide covers MLflow 3.x as of early 2026. MLflow releases monthly; check the [changelog](https://github.com/mlflow/mlflow/blob/master/CHANGELOG.md) for the latest features.*
