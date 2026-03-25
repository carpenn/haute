"""Tests for exploratory-analysis node registration and profiling routes."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from haute._builders import _build_node_fn
from haute._config_validation import warn_unrecognized_config_keys
from haute._types import ExploratoryAnalysisConfig, GraphEdge, GraphNode, NodeData, NodeType, PipelineGraph


def _make_exploratory_node(
    label: str = "EDA Node",
    config: dict | None = None,
) -> GraphNode:
    return GraphNode(
        id="eda1",
        data=NodeData(
            label=label,
            nodeType=NodeType.EXPLORATORY_ANALYSIS,
            config=config or {},
        ),
    )


def _build(node: GraphNode, sources: list[str] | None = None) -> tuple[str, Callable, bool]:
    return _build_node_fn(node, source_names=sources or ["upstream"])


class TestExploratoryAnalysisNodeType:
    def test_enum_value(self) -> None:
        assert NodeType.EXPLORATORY_ANALYSIS == "exploratoryAnalysis"

    def test_string_value(self) -> None:
        assert str(NodeType.EXPLORATORY_ANALYSIS) == "exploratoryAnalysis"


class TestExploratoryAnalysisConfig:
    def test_config_is_typed_dict(self) -> None:
        cfg: ExploratoryAnalysisConfig = {
            "fieldRoles": {"claim_id": "claim_key", "target_value": "target"}
        }
        assert cfg["fieldRoles"]["claim_id"] == "claim_key"

    def test_config_total_false_allows_partial(self) -> None:
        cfg: ExploratoryAnalysisConfig = {}
        assert isinstance(cfg, dict)


class TestExploratoryAnalysisConfigValidation:
    def test_valid_config_produces_no_exception(self) -> None:
        warn_unrecognized_config_keys(
            NodeType.EXPLORATORY_ANALYSIS,
            {"fieldRoles": {"claim_id": "claim_key"}},
        )

    def test_unknown_key_triggers_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.WARNING):
            warn_unrecognized_config_keys(
                NodeType.EXPLORATORY_ANALYSIS,
                {"fieldRoles": {"claim_id": "claim_key"}, "bogusKey": True},
            )
        assert isinstance(caplog.records, list)


class TestExploratoryAnalysisBuilder:
    def test_returns_tuple_of_three(self) -> None:
        result = _build(_make_exploratory_node())
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_is_not_a_source_node(self) -> None:
        _, _, is_source = _build(_make_exploratory_node())
        assert is_source is False

    def test_is_passthrough(self) -> None:
        _, fn, _ = _build(_make_exploratory_node())
        upstream = pl.LazyFrame({"claim_id": [1, 2], "target": [10.0, 20.0]})
        collected = fn(upstream).collect()
        assert collected.shape == (2, 2)
        assert collected.columns == ["claim_id", "target"]


@pytest.fixture()
def exploratory_project(tmp_path: Path) -> tuple[Path, str]:
    data_path = tmp_path / "claims.parquet"
    pl.DataFrame(
        {
            "policy_id": ["P1", "P2", "P3", "P4", "P5", "P6"],
            "claim_id": ["C1", "C2", "C3", "C4", "C5", "C6"],
            "accident_date": [
                "2024-01-01",
                "2024-01-10",
                "2024-01-15",
                "2024-02-01",
                "2024-02-10",
                "2024-03-01",
            ],
            "feature_band": ["A", "A", "?", "B", "UNKNOWN", "C"],
            "target_value": [100.0, 120.0, 130.0, 140.0, 150.0, 3000.0],
        }
    ).with_columns(pl.col("accident_date").str.strptime(pl.Date, strict=False)).write_parquet(data_path)
    return tmp_path, data_path.as_posix()


@pytest.fixture()
def client(exploratory_project: tuple[Path, str], monkeypatch: pytest.MonkeyPatch) -> TestClient:
    project_dir, _ = exploratory_project
    monkeypatch.chdir(project_dir)
    from haute.server import app

    return TestClient(app)


def _build_graph(data_path: str) -> PipelineGraph:
    source = GraphNode(
        id="source",
        data=NodeData(
            label="Source",
            nodeType=NodeType.DATA_SOURCE,
            config={"path": data_path, "sourceType": "flat_file"},
        ),
    )
    eda = GraphNode(
        id="eda",
        data=NodeData(
            label="EDA",
            nodeType=NodeType.EXPLORATORY_ANALYSIS,
            config={
                "fieldRoles": {
                    "policy_id": "policy_key",
                    "claim_id": "claim_key",
                    "accident_date": "accident_date",
                    "feature_band": "covariate",
                    "target_value": "target",
                }
            },
        ),
    )
    return PipelineGraph(nodes=[source, eda], edges=[GraphEdge(id="e1", source="source", target="eda")])


class TestExploratoryAnalysisRoutes:
    def test_analysis_route_returns_all_tabs(
        self,
        client: TestClient,
        exploratory_project: tuple[Path, str],
    ) -> None:
        _, data_path = exploratory_project
        graph = _build_graph(data_path)

        resp = client.post(
            "/api/pipeline/exploratory-analysis",
            json={"graph": graph.model_dump(), "node_id": "eda"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["row_count"] == 6
        assert any(row["field"] == "target_value" for row in data["descriptive_statistics"])
        assert any(row["field"] == "target_value" for row in data["outliers_inliers"])
        assert any(row["field"] == "feature_band" for row in data["disguised_missings"])
        assert "auto" in data["correlations"]["types"]
        assert data["one_way_options"][0]["field"] == "accident_date"
        assert data["chart"]["points"]

    def test_one_way_chart_route_recomputes_selected_axis(
        self,
        client: TestClient,
        exploratory_project: tuple[Path, str],
    ) -> None:
        _, data_path = exploratory_project
        graph = _build_graph(data_path)

        resp = client.post(
            "/api/pipeline/exploratory-analysis/one-way",
            json={
                "graph": graph.model_dump(),
                "node_id": "eda",
                "x_field": "feature_band",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["chart"]["x_field"] == "feature_band"
        assert data["chart"]["bar_label"] == "Unique claim_id"
        assert len(data["chart"]["points"]) >= 1
