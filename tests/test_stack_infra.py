"""Regression tests for stack infrastructure added in PR #42.

Validates compose file shape, config file presence, and DAG syntax without
requiring any of the services to be running.
"""

import ast
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_compose(name: str) -> dict:
    path = PROJECT_ROOT / name
    with path.open() as f:
        return yaml.safe_load(f)


class TestComposeFilesParse:
    """Every shipped compose file should be valid YAML."""

    @pytest.mark.parametrize(
        "compose_file",
        [
            "docker-compose.yml",
            "docker-compose-spark41.yml",
            "docker-compose-kafka.yml",
            "docker-compose-unity-catalog.yml",
            "docker-compose-mlflow.yml",
            "docker-compose-airflow.yml",
            "docker-compose-notebooks.yml",
        ],
    )
    def test_compose_file_parses(self, compose_file):
        doc = _load_compose(compose_file)
        assert isinstance(doc, dict), f"{compose_file} did not parse to a dict"
        assert "services" in doc, f"{compose_file} missing services key"
        assert doc["services"], f"{compose_file} has empty services"


class TestMlflowCompose:
    """docker-compose-mlflow.yml ships MLflow 3.x (gateway is embedded in
    the tracking server as of 3.9+, mounted via MLFLOW_GATEWAY_CONFIG)."""

    def setup_method(self):
        self.compose = _load_compose("docker-compose-mlflow.yml")
        self.services = self.compose["services"]

    def test_tracking_service_present(self):
        names = set(self.services)
        assert names & {"mlflow", "mlflow-tracking", "mlflow-server"}, (
            f"No MLflow tracking service in {names}"
        )

    def test_tracking_exposes_port_5000(self):
        svc = self.services.get("mlflow") or self.services.get("mlflow-tracking")
        assert svc is not None
        ports = svc.get("ports", [])
        assert any("5000" in str(p) for p in ports), (
            f"MLflow tracking should expose 5000, got {ports}"
        )

    def test_gateway_config_mounted(self):
        """Gateway config must be reachable to the tracking container."""
        svc = self.services.get("mlflow") or self.services.get("mlflow-tracking")
        assert svc is not None
        env = svc.get("environment", {})
        # environment can be dict or list of "K=V"
        if isinstance(env, list):
            env = dict(e.split("=", 1) for e in env if "=" in e)
        assert "MLFLOW_GATEWAY_CONFIG" in env, (
            "MLFLOW_GATEWAY_CONFIG env var missing — gateway routes would not load"
        )

    def test_gateway_config_file_exists(self):
        assert (PROJECT_ROOT / "config" / "mlflow" / "gateway-config.yml").is_file()


class TestUnityCatalogCompose:
    """UC moved from 8080 to 8081; regression-guard the port."""

    def test_uc_listens_on_8081(self):
        compose = _load_compose("docker-compose-unity-catalog.yml")
        ports = []
        for svc in compose["services"].values():
            ports.extend(svc.get("ports", []))
        # Ports are usually "HOST:CONTAINER" strings
        host_ports = []
        for p in ports:
            if isinstance(p, str) and ":" in p:
                host_ports.append(p.split(":")[0].strip('"'))
            elif isinstance(p, dict):
                host_ports.append(str(p.get("published", "")))
        assert "8081" in host_ports, (
            f"UC compose host ports {host_ports} should include 8081"
        )
        assert "8080" not in host_ports, (
            "UC should not collide with Spark 4.0 UI on 8080"
        )


class TestSparkConfigExamples:
    """UC + Delta example configs should ship alongside the base example."""

    @pytest.mark.parametrize(
        "config_file",
        [
            "config/spark/spark-defaults-uc.conf.example",
            "config/spark/spark-defaults-delta.conf.example",
        ],
    )
    def test_example_config_exists_and_nonempty(self, config_file):
        path = PROJECT_ROOT / config_file
        assert path.is_file(), f"Missing {config_file}"
        assert path.stat().st_size > 0, f"{config_file} is empty"

    def test_delta_example_references_delta_catalog(self):
        text = (
            PROJECT_ROOT / "config/spark/spark-defaults-delta.conf.example"
        ).read_text()
        assert "delta" in text.lower(), "Delta example should mention delta"
        assert "DeltaCatalog" in text or "delta.sql.catalog" in text.lower(), (
            "Delta example should configure a Delta-capable catalog"
        )


class TestSdpDag:
    """dags/sdp_pipeline.py should be valid Python that Airflow can import."""

    DAG_PATH = PROJECT_ROOT / "dags" / "sdp_pipeline.py"

    def test_dag_file_exists(self):
        assert self.DAG_PATH.is_file()

    def test_dag_parses(self):
        source = self.DAG_PATH.read_text()
        ast.parse(source)

    def test_dag_uses_spark_submit_operator(self):
        # No native SDP operator upstream yet — verify we fell back to
        # SparkSubmitOperator as documented.
        source = self.DAG_PATH.read_text()
        assert "SparkSubmitOperator" in source, (
            "sdp_pipeline.py should use SparkSubmitOperator "
            "(no native SDP operator exists upstream yet)"
        )


class TestSdpDataSourcesDoc:
    """SDP compatibility matrix should ship."""

    def test_doc_exists(self):
        assert (PROJECT_ROOT / "docs" / "guides" / "sdp-data-sources.md").is_file()


class TestAgentsReference:
    """AGENTS.md should exist at the repo root for AI assistants."""

    def test_agents_reference_exists(self):
        path = PROJECT_ROOT / "AGENTS.md"
        assert path.is_file()
        content = path.read_text()
        assert "Spark 4.1" in content, "AGENTS.md should reference Spark 4.1"


class TestBenchmarksPackage:
    """benchmarks/ should be importable as a runnable module."""

    def test_package_importable(self):
        import importlib

        mod = importlib.import_module("benchmarks")
        assert mod is not None

    def test_main_importable(self):
        import importlib

        mod = importlib.import_module("benchmarks.__main__")
        assert mod is not None
