#!/usr/bin/env bash
# Act 7: MLflow Agents
# Guardian, Analyst, Autopilot — run from host, not spark-submit
#
# Requires: export ANTHROPIC_API_KEY=sk-...
# Or for local LLM: export LLM_PROVIDER=openai LLM_MODEL=qwen2.5:14b OPENAI_BASE_URL=http://localhost:11434/v1

set -e
cd /home/lnc/lakehouse-stack

if [ -z "$ANTHROPIC_API_KEY" ] && [ -z "$LLM_PROVIDER" ]; then
    echo "Using Ollama (llama3.1:8b) — no API key needed."
fi

export SPARK_REMOTE="${SPARK_REMOTE:-sc://localhost:15002}"
export MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI:-}"
export LLM_MODEL="${LLM_MODEL:-qwen3-coder:30b}"

echo "=== MLflow — Health Check ==="
curl -s http://localhost:5000/health | python3 -m json.tool 2>/dev/null || echo "MLflow at http://localhost:5000"

echo ""
echo "=== Guardian Agent — Table Health Check ==="
python3 scripts/demos/overarchitected/06a_mlflow_guardian.py

echo ""
echo "=== MLflow UI — Traces visible at http://localhost:5000 ==="
echo "  Every LLM call, every tool invocation, every token — logged."
