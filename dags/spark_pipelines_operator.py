"""
Vendored SparkPipelinesOperator + SparkPipelinesHook
====================================================

Self-contained re-implementation of the operator merged into Apache Airflow
in March 2026 (PR 61681). Put it in the Dags folder (which Airflow adds to
sys.path) and import it with `from spark_pipelines_operator import ...`.

Why vendor: the public provider release that carries the upstream operator
may not match the version pinned in docker/airflow/Dockerfile. Vendoring
removes that coupling. If you later upgrade the provider and want to switch
to the upstream class, change the import lines in your Dags.

What this copy actually does (it differs from upstream on purpose):
  * pipeline_spec / pipeline_root: which spec to run (exactly one)
  * pipeline_command: "run" or "dry-run"
  * full_refresh, refresh: mapped to --full-refresh-all / --refresh
  * env_vars: merged into the subprocess environment; set SPARK_REMOTE here
    (or in the worker env) to target a Spark Connect server
  * application_args: exposed to transformations as the PIPELINE_ARGS env var
  * Templates: pipeline_spec, pipeline_root, pipeline_command, conf,
    env_vars, application_args

It invokes pyspark's pipelines CLI module directly instead of spark-submit
(see execute() for why). As a result, conn_id, conf, num_executors,
executor_cores, executor_memory, driver_memory, deploy_mode, verbose and
openlineage_enabled are accepted for signature parity with upstream but are
NOT applied. Put Spark conf in the pipeline spec's `configuration:` block.
on_kill() does not terminate the subprocess.
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
        "pipeline_root",
        "pipeline_command",
        "conf",
        "env_vars",
        "application_args",
    )
    ui_color = "#1FBAD6"

    def __init__(
        self,
        *,
        pipeline_spec: str | None = None,
        pipeline_root: str | None = None,
        pipeline_command: str = "run",
        conn_id: str = "spark_default",
        conf: dict[str, Any] | None = None,
        env_vars: dict[str, str] | None = None,
        num_executors: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        deploy_mode: str | None = None,
        full_refresh: bool = False,
        refresh: list[str] | None = None,
        application_args: list[str] | None = None,
        openlineage_enabled: bool = False,
        verbose: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if pipeline_command not in {"run", "dry-run"}:
            raise ValueError(
                f"pipeline_command must be 'run' or 'dry-run', got {pipeline_command!r}"
            )
        if bool(pipeline_spec) == bool(pipeline_root):
            raise ValueError("Provide exactly one of pipeline_spec or pipeline_root")
        self.pipeline_spec = pipeline_spec
        self.pipeline_root = pipeline_root
        self.pipeline_command = pipeline_command
        self.conn_id = conn_id
        self.conf = conf or {}
        self.env_vars = env_vars or {}
        self.num_executors = num_executors
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.deploy_mode = deploy_mode
        self.full_refresh = full_refresh
        self.refresh = refresh or []
        self.application_args = application_args or []
        self.openlineage_enabled = openlineage_enabled
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
            [
                sys.executable,
                "-c",
                "import os, pyspark; "
                "print(os.path.join(os.path.dirname(pyspark.__file__), 'pipelines', 'cli.py'))",
            ],
            text=True,
        ).strip()

        spec = self.pipeline_spec or os.path.join(
            self.pipeline_root, "spark-pipeline.yml"
        )
        cmd = [sys.executable, cli_py, self.pipeline_command, "--spec", spec]
        if self.pipeline_command == "run":
            if self.full_refresh:
                cmd.append("--full-refresh-all")
            if self.refresh:
                cmd += ["--refresh", ",".join(self.refresh)]
        if self.application_args:
            # The pipelines CLI has no passthrough args; surface them via env
            # so transformations can read them (e.g. os.environ["PIPELINE_ARGS"]).
            self.log.warning(
                "application_args are not supported by the spark-pipelines CLI; "
                "exposing them as PIPELINE_ARGS env var instead: %s",
                self.application_args,
            )
        env = {**os.environ, **(self.env_vars or {})}
        if self.application_args:
            env["PIPELINE_ARGS"] = " ".join(str(a) for a in self.application_args)
        self.log.info(
            "Running (SPARK_REMOTE=%s): %s",
            env.get("SPARK_REMOTE", "<unset>"),
            " ".join(cmd),
        )
        proc = subprocess.run(cmd, env=env)
        if proc.returncode != 0:
            raise AirflowException(
                f"spark-pipelines {self.pipeline_command} failed (exit {proc.returncode})"
            )

    def on_kill(self) -> None:
        if self._hook is not None:
            self._hook.on_kill()
