"""Regression tests for the demo suites added in PR #43.

Lightweight checks that don't require a live Spark cluster: every demo
parses as valid Python, no show-era framing leaked through, and the
READMEs exist.
"""

import ast
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SHOWCASE = PROJECT_ROOT / "scripts" / "demos" / "showcase"
AGENTS = PROJECT_ROOT / "scripts" / "demos" / "mlflow-agents"


class TestDemoSuitesPresent:
    """Both new demo directories exist with READMEs."""

    def test_showcase_dir_has_readme(self):
        assert (SHOWCASE / "README.md").is_file()

    def test_mlflow_agents_dir_has_readme(self):
        assert (AGENTS / "README.md").is_file()

    def test_top_level_demos_readme_indexes_suites(self):
        readme = (PROJECT_ROOT / "scripts" / "demos" / "README.md").read_text()
        assert "showcase/" in readme
        assert "mlflow-agents/" in readme


@pytest.mark.parametrize(
    "py_file",
    sorted(SHOWCASE.glob("*.py")) + sorted(AGENTS.glob("*.py")),
    ids=lambda p: str(p.relative_to(PROJECT_ROOT)),
)
def test_demo_parses(py_file):
    """Each demo script must be importable Python — even if it can't
    execute without a live cluster, it should at least parse."""
    ast.parse(py_file.read_text())


@pytest.mark.parametrize(
    "demo_dir",
    [SHOWCASE, AGENTS],
    ids=lambda p: p.name,
)
def test_no_show_framing_leaked(demo_dir):
    """sed-stripped in Phase 3 — guard against regressions. Holly and Nick
    are the original show hosts; OverArchitected was the episode title."""
    forbidden = ("OverArchitected", "Holly", "Nick", "smuggled out")
    for py_file in demo_dir.glob("*.py"):
        text = py_file.read_text()
        for term in forbidden:
            assert term not in text, (
                f"{py_file.name} still contains show framing: '{term}'"
            )


class TestShowcaseBuildingBlocks:
    """Each named building block from the PR description should exist."""

    @pytest.mark.parametrize(
        "filename",
        [
            "otf_portability.py",
            "iceberg_variant.py",
            "unity_catalog_setup.py",
            "spark41_feature_tour.py",
            "spark_cluster_diagnostic.py",
            "streaming_udtf.py",
            "sdp_showcase.py",
            "sdp_demo.py",
            "realtime_mode.py",
            "airflow_sdp.py",
            "spark_connect.py",
            "spark_on_k8s.sh",
            "sdp-pipeline.yml",
        ],
    )
    def test_building_block_exists(self, filename):
        assert (SHOWCASE / filename).is_file()


class TestMlflowAgents:
    """Three named agents: guardian, analyst, autopilot."""

    @pytest.mark.parametrize("name", ["guardian", "analyst", "autopilot"])
    def test_agent_exists(self, name):
        assert (AGENTS / f"{name}.py").is_file()
