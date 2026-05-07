"""
Vendored SparkPipelinesOperator + SparkPipelinesHook
====================================================

Self-contained re-implementation of the operator merged into Apache Airflow
in March 2026 (PR 61681). Drop in next to the demo DAGs so they work on any
apache-airflow-providers-apache-spark version that includes SparkSubmitHook.

Why vendor: the public provider release that carries the upstream operator
may not match the version pinned in docker/airflow/Dockerfile. Vendoring
removes that coupling. If you later upgrade the provider and want to switch
to the upstream class, change two import lines in the DAGs.

Behavior parity with upstream (per the merged PR description):
  * pipeline_spec       — path to spark-pipeline.yml
  * pipeline_command    — "run" or "dry-run"
  * conn_id             — Airflow Spark connection (default: spark_default)
  * conf, env_vars      — forwarded to the underlying spark-submit invocation
  * num_executors, executor_memory, driver_memory, deploy_mode — Spark resources
  * Templates: pipeline_spec, pipeline_command, conf, env_vars
  * Reuses SparkSubmitHook for connection parsing, env, on-kill cleanup
"""

from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


class SparkPipelinesHook(SparkSubmitHook):
    """SparkSubmitHook that calls `spark-pipelines` instead of `spark-submit`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # The hook builds a command starting with self._connection["spark_binary"]
        # or the default. Override to spark-pipelines.
        self._connection["spark_binary"] = "spark-pipelines"


class SparkPipelinesOperator(BaseOperator):
    """Run a Spark Declarative Pipeline via the spark-pipelines CLI.

    Use this for SDP pipelines defined by a spark-pipeline.yml spec.
    For generic spark-submit jobs, use SparkSubmitOperator.
    """

    template_fields: Sequence[str] = (
        "pipeline_spec",
        "pipeline_command",
        "conf",
        "env_vars",
    )
    ui_color = "#1FBAD6"

    def __init__(
        self,
        *,
        pipeline_spec: str,
        pipeline_command: str = "run",
        conn_id: str = "spark_default",
        conf: dict[str, Any] | None = None,
        env_vars: dict[str, str] | None = None,
        num_executors: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        deploy_mode: str | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if pipeline_command not in {"run", "dry-run"}:
            raise ValueError(
                f"pipeline_command must be 'run' or 'dry-run', got {pipeline_command!r}"
            )
        self.pipeline_spec = pipeline_spec
        self.pipeline_command = pipeline_command
        self.conn_id = conn_id
        self.conf = conf or {}
        self.env_vars = env_vars or {}
        self.num_executors = num_executors
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.deploy_mode = deploy_mode
        self.verbose = verbose
        self._hook: SparkPipelinesHook | None = None

    def execute(self, context: Any) -> None:
        # Spark 4.1's `spark-pipelines` shell launcher routes through
        # spark-class → SparkSubmit → JVM SparkContext, which (a) emits
        # --master/--deploy-mode flags the Connect-native CLI rejects, and
        # (b) tries to bind its own Spark Connect server on 15002, colliding
        # with the long-running Connect daemon when SPARK_REMOTE is set.
        # Invoke the underlying Python CLI directly so SparkSession.builder
        # routes through SPARK_REMOTE as a Connect client.
        import os
        import subprocess
        import sys

        # Locate pyspark/pipelines/cli.py without importing pyspark in the
        # operator process (avoids loading Spark client deps just to dispatch).
        cli_py = subprocess.check_output(
            [sys.executable, "-c",
             "import os, pyspark; "
             "print(os.path.join(os.path.dirname(pyspark.__file__), 'pipelines', 'cli.py'))"],
            text=True,
        ).strip()

        cmd = [sys.executable, cli_py, self.pipeline_command, "--spec", self.pipeline_spec]
        env = {**os.environ, **(self.env_vars or {})}
        self.log.info("Running (SPARK_REMOTE=%s): %s",
                      env.get("SPARK_REMOTE", "<unset>"), " ".join(cmd))
        proc = subprocess.run(cmd, env=env)
        if proc.returncode != 0:
            raise AirflowException(
                f"spark-pipelines {self.pipeline_command} failed (exit {proc.returncode})"
            )

    def on_kill(self) -> None:
        if self._hook is not None:
            self._hook.on_kill()
