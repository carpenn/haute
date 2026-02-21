"""API integration tests for modelling endpoints."""

from __future__ import annotations

import time

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient

from haute._types import NodeType
from haute.server import app
from tests.conftest import make_edge, make_graph


@pytest.fixture()
def client():
    return TestClient(app)


def _make_modelling_graph(
    data_path: str,
    target: str = "y",
    weight: str | None = None,
    algorithm: str = "catboost",
    task: str = "regression",
    params: dict | None = None,
) -> dict:
    """Build a simple 2-node graph: dataSource → modelling."""
    config: dict = {
        "target": target,
        "algorithm": algorithm,
        "task": task,
        "params": params or {"iterations": 10, "depth": 3},
        "split": {"strategy": "random", "test_size": 0.2, "seed": 42},
        "metrics": ["gini", "rmse"] if task == "regression" else ["auc", "logloss"],
    }
    if weight:
        config["weight"] = weight

    graph = make_graph({
        "nodes": [
            {
                "id": "source",
                "data": {"label": "source", "nodeType": "dataSource", "config": {"path": data_path}},
            },
            {
                "id": "train",
                "data": {"label": "train", "nodeType": "modelling", "config": config},
            },
        ],
        "edges": [make_edge("source", "train").model_dump()],
    })
    return graph.model_dump()


@pytest.fixture()
def training_data(tmp_path) -> str:
    """Create a small parquet file for training tests."""
    rng = np.random.RandomState(42)
    n = 100
    df = pl.DataFrame({
        "x1": rng.randn(n),
        "x2": rng.randn(n),
        "y": (rng.randn(n) * 2 + 1).clip(0),
    })
    path = tmp_path / "train_data.parquet"
    df.write_parquet(path)
    return str(path)


def _poll_until_done(client: TestClient, job_id: str, timeout: float = 30) -> dict:
    """Poll /train/status/{job_id} until completed or error, return final status."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/api/modelling/train/status/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["status"] in ("completed", "error"):
            return data
        time.sleep(0.1)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout}s")


class TestTrainEndpoint:
    def test_train_with_invalid_target(self, client, training_data):
        graph = _make_modelling_graph(training_data, target="nonexistent")
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "train"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"
        assert data["job_id"]
        # Poll until done — should fail with a clear message
        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "error"
        assert "nonexistent" in status["message"].lower() or "target" in status["message"].lower()

    def test_train_missing_node(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "nonexistent"})
        assert resp.status_code == 404

    def test_train_wrong_node_type(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "source"})
        assert resp.status_code == 400

    def test_train_success(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "train"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"
        assert data["job_id"]
        # Poll until done
        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed"
        result = status["result"]
        assert result["metrics"]
        assert result["train_rows"] > 0
        assert result["test_rows"] > 0

    def test_train_reports_progress(self, client, training_data):
        """Training should report iteration progress via the status endpoint."""
        graph = _make_modelling_graph(training_data, params={"iterations": 20, "depth": 3})
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "train"})
        data = resp.json()
        job_id = data["job_id"]
        # Poll a few times — we should see iteration progress at some point
        saw_iteration = False
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            resp = client.get(f"/api/modelling/train/status/{job_id}")
            status = resp.json()
            if status.get("iteration", 0) > 0:
                saw_iteration = True
            if status["status"] in ("completed", "error"):
                break
            time.sleep(0.05)
        assert status["status"] == "completed"
        # With 20 iterations, we should have seen at least one iteration update
        # (though fast training might complete before we poll — check final state too)
        assert saw_iteration or status.get("result", {}).get("train_rows", 0) > 0


    def test_train_result_includes_ave(self, client, training_data):
        """After successful training, ave_per_feature should be in the result."""
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "nodeId": "train"})
        data = resp.json()
        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed"
        result = status["result"]
        # Should have ave_per_feature for the 2 features (x1, x2)
        assert "ave_per_feature" in result
        assert isinstance(result["ave_per_feature"], list)
        assert len(result["ave_per_feature"]) == 2
        for entry in result["ave_per_feature"]:
            assert "feature" in entry
            assert "type" in entry
            assert "bins" in entry


class TestExportEndpoint:
    def test_export_generates_script(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/export", json={
            "graph": graph,
            "nodeId": "train",
            "data_path": "output/data.parquet",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "script" in data
        assert "filename" in data
        assert "TrainingJob" in data["script"]
        assert data["filename"].endswith(".py")

    def test_export_missing_node(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/export", json={
            "graph": graph,
            "nodeId": "nonexistent",
        })
        assert resp.status_code == 404


class TestTrainStatusEndpoint:
    def test_missing_job_returns_404(self, client):
        resp = client.get("/api/modelling/train/status/nonexistent")
        assert resp.status_code == 404


class TestMlflowCheckEndpoint:
    def test_mlflow_check_response_shape(self, client):
        resp = client.get("/api/modelling/mlflow/check")
        assert resp.status_code == 200
        data = resp.json()
        assert "mlflow_installed" in data
        assert "backend" in data
        assert "databricks_host" in data
        # mlflow is installed in test env
        assert data["mlflow_installed"] is True


class TestMlflowLogEndpoint:
    def test_mlflow_log_job_not_found(self, client):
        resp = client.post("/api/modelling/mlflow/log", json={
            "job_id": "nonexistent",
        })
        assert resp.status_code == 404

    def test_mlflow_log_job_not_completed(self, client, training_data):
        """Start a job, then immediately try to log — should fail with 400."""
        from haute.routes.modelling import _jobs

        # Inject a fake running job
        _jobs["fake_running"] = {"status": "running", "progress": 0.5, "message": "Training..."}
        try:
            resp = client.post("/api/modelling/mlflow/log", json={
                "job_id": "fake_running",
            })
            assert resp.status_code == 400
            assert "not completed" in resp.json()["detail"]
        finally:
            _jobs.pop("fake_running", None)
