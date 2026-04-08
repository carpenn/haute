"""Route tests for backend-only analysis endpoints."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import polars as pl

from haute.routes.eda import _run_pipeline_and_collect
from haute.routes.pipeline import triangle_node
from haute.schemas import NodeResult, TriangleRequest
from tests.conftest import make_graph


def test_run_pipeline_and_collect_requests_uncapped_preview(monkeypatch) -> None:
    """EDA should request the full dataset from the executor."""

    captured: dict[str, object] = {}

    def _fake_execute_graph(*args, **kwargs):
        captured.update(kwargs)
        return {
            "eda": NodeResult(
                status="ok",
                row_count=3,
                column_count=1,
                preview=[{"x": 1}, {"x": 2}, {"x": 3}],
            )
        }

    monkeypatch.setattr("haute.executor.execute_graph", _fake_execute_graph)

    graph = make_graph({"nodes": [], "edges": []})
    df = _run_pipeline_and_collect(graph, "eda", "live")

    assert captured["row_limit"] == 0
    assert captured["max_preview_rows"] is None
    assert df.equals(pl.DataFrame({"x": [1, 2, 3]}))


def test_triangle_route_requests_uncapped_preview(monkeypatch) -> None:
    """Triangle Viewer should process the full upstream dataset."""

    captured: dict[str, object] = {}

    def _fake_execute_graph(*args, **kwargs):
        captured.update(kwargs)
        return {
            "triangle": NodeResult(
                status="ok",
                row_count=2,
                column_count=3,
                preview=[
                    {"origin": "2020", "development": "12", "value": 10.0},
                    {"origin": "2020", "development": "24", "value": 15.0},
                ],
            )
        }

    process_triangle = MagicMock(
        return_value={
            "origins": ["2020"],
            "developments": ["12", "24"],
            "values": [[10.0, 15.0]],
        }
    )

    monkeypatch.setattr("haute.executor.execute_graph", _fake_execute_graph)
    monkeypatch.setattr("haute.routes._triangle_service.process_triangle", process_triangle)

    graph = make_graph(
        {
            "nodes": [
                {
                    "id": "triangle",
                    "data": {
                        "label": "triangle",
                        "nodeType": "triangleViewer",
                        "config": {
                            "originField": "origin",
                            "developmentField": "development",
                            "valueField": "value",
                        },
                    },
                }
            ],
            "edges": [],
        }
    )

    response = asyncio.run(
        triangle_node(
            TriangleRequest(
                graph=graph,
                node_id="triangle",
                source="live",
            )
        )
    )

    assert response.status == "ok"
    assert captured["row_limit"] == 0
    assert captured["max_preview_rows"] is None
    process_triangle.assert_called_once()
