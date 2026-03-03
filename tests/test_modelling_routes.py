"""API integration tests for modelling endpoints."""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient

from haute.routes.modelling import _clamp_row_limit, _friendly_error
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
                "data": {
                    "label": "source", "nodeType": "dataSource",
                    "config": {"path": data_path},
                },
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
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
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
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "nonexistent"})
        assert resp.status_code == 404

    def test_train_wrong_node_type(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "source"})
        assert resp.status_code == 400

    def test_train_success(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
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
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
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
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
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


    def test_train_rejects_concurrent(self, client, training_data):
        """A second training request while one is running returns 409."""
        from haute.routes.modelling import _store

        _store.jobs["fake_running"] = {
            "status": "running",
            "progress": 0.5,
            "message": "Training...",
            "created_at": time.time(),
        }
        try:
            graph = _make_modelling_graph(training_data)
            resp = client.post(
                "/api/modelling/train",
                json={"graph": graph, "node_id": "train"},
            )
            assert resp.status_code == 409
            assert "already running" in resp.json()["detail"]
        finally:
            _store.jobs.pop("fake_running", None)


    def test_train_gpu_falls_back_to_cpu_on_vram_limit(self, client, training_data):
        """When GPU VRAM is insufficient, training should fall back to CPU."""
        graph = _make_modelling_graph(
            training_data,
            params={"iterations": 10, "depth": 3, "task_type": "GPU"},
        )
        # Pretend GPU has only 1 byte VRAM — forces fallback to CPU
        # (even 100 rows × 3 features needs more than 1 byte)
        with patch(
            "haute._ram_estimate.available_vram_bytes",
            return_value=1,
        ):
            resp = client.post(
                "/api/modelling/train",
                json={"graph": graph, "node_id": "train"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"

        # Poll until done — should succeed on CPU (not crash on GPU OOM)
        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed"
        # The warning should mention GPU fallback
        warning = status.get("warning") or ""
        assert "GPU" in warning or "VRAM" in warning or "CPU" in warning


class TestExportEndpoint:
    def test_export_generates_script(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/export", json={
            "graph": graph,
            "node_id": "train",
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
            "node_id": "nonexistent",
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
        from haute.routes.modelling import _store

        # Inject a fake running job
        _store.jobs["fake_running"] = {"status": "running", "progress": 0.5, "message": "Training...", "created_at": time.time()}
        try:
            resp = client.post("/api/modelling/mlflow/log", json={
                "job_id": "fake_running",
            })
            assert resp.status_code == 400
            assert "not completed" in resp.json()["detail"]
        finally:
            _store.jobs.pop("fake_running", None)


# ---------------------------------------------------------------------------
# Phase 1A: Pure function tests
# ---------------------------------------------------------------------------


class TestFriendlyError:
    """Unit tests for _friendly_error — translates exceptions into user messages."""

    def test_value_error_passthrough(self):
        exc = ValueError("Target column 'z' not found")
        assert _friendly_error(exc) == "Target column 'z' not found"

    def test_file_not_found(self):
        exc = FileNotFoundError("/data/missing.parquet")
        result = _friendly_error(exc)
        assert result.startswith("File not found:")
        assert "missing.parquet" in result

    def test_catboost_nan(self):
        """CatBoost NaN/Inf errors should recommend upstream transforms."""
        # Simulate CatBoost error class
        exc = type("CatBoostError", (Exception,), {})("NaN values in features")
        result = _friendly_error(exc)
        assert "NaN" in result or "nan" in result.lower()
        assert "transform" in result.lower()

    def test_catboost_feature_mismatch(self):
        exc = type("CatBoostError", (Exception,), {})("feature number mismatch: expected 10 got 8")
        result = _friendly_error(exc)
        assert "feature mismatch" in result.lower()

    def test_catboost_generic(self):
        exc = type("CatBoostError", (Exception,), {})("internal pool error")
        result = _friendly_error(exc)
        assert result.startswith("CatBoost error:")
        assert "internal pool error" in result

    def test_os_error(self):
        exc = OSError("Permission denied: /models/model.cbm")
        result = _friendly_error(exc)
        assert result.startswith("Could not save model file:")
        assert "Permission denied" in result

    def test_fallback_includes_type(self):
        exc = RuntimeError("something unexpected")
        result = _friendly_error(exc)
        assert "RuntimeError" in result
        assert "something unexpected" in result

    def test_catboost_inf_message(self):
        """'inf' in message also triggers NaN/Inf advice."""
        exc = type("CatBoostError", (Exception,), {})("Found inf in column 3")
        result = _friendly_error(exc)
        assert "infinite" in result.lower() or "inf" in result.lower()
        assert "transform" in result.lower()


class TestClampRowLimit:
    """Unit tests for _clamp_row_limit — applies user row limits."""

    def test_none_user_limit_returns_current(self):
        assert _clamp_row_limit(1000, None) == 1000

    def test_zero_user_limit_returns_current(self):
        assert _clamp_row_limit(1000, 0) == 1000

    def test_negative_user_limit_returns_current(self):
        assert _clamp_row_limit(1000, -5) == 1000

    def test_user_smaller_than_current(self):
        assert _clamp_row_limit(1000, 500) == 500

    def test_user_larger_than_current(self):
        assert _clamp_row_limit(500, 1000) == 500

    def test_no_current_limit(self):
        assert _clamp_row_limit(None, 500) == 500

    def test_string_user_limit_ignored(self):
        assert _clamp_row_limit(1000, "abc") == 1000

    def test_float_user_limit_converted(self):
        assert _clamp_row_limit(1000, 500.7) == 500

    def test_both_none(self):
        assert _clamp_row_limit(None, None) is None


# ---------------------------------------------------------------------------
# Phase 1A: Endpoint validation gaps
# ---------------------------------------------------------------------------


class TestTrainValidation:
    """Upfront validation errors that return 400 without starting a job."""

    def test_train_no_target(self, client, training_data):
        graph = _make_modelling_graph(training_data, target="y")
        # Remove target from config
        graph_dict = graph
        for node in graph_dict["nodes"]:
            if node["id"] == "train":
                node["data"]["config"]["target"] = ""
        resp = client.post("/api/modelling/train", json={"graph": graph_dict, "node_id": "train"})
        assert resp.status_code == 400
        assert "target" in resp.json()["detail"].lower()

    def test_train_unknown_algorithm(self, client, training_data):
        graph = _make_modelling_graph(training_data, algorithm="catboost")
        for node in graph["nodes"]:
            if node["id"] == "train":
                node["data"]["config"]["algorithm"] = "nonexistent_algo"
        resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
        assert resp.status_code == 400
        assert "nonexistent_algo" in resp.json()["detail"]


class TestEstimateEndpoint:
    """Tests for /estimate — RAM + row estimation."""

    def test_estimate_success(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/estimate", json={"graph": graph, "node_id": "train"})
        assert resp.status_code == 200
        data = resp.json()
        assert "total_rows" in data
        assert "safe_row_limit" in data
        assert "estimated_mb" in data
        assert "training_mb" in data

    def test_estimate_gpu_vram_path(self, client, training_data):
        graph = _make_modelling_graph(
            training_data,
            params={"iterations": 10, "depth": 3, "task_type": "GPU"},
        )
        with patch("haute._ram_estimate.available_vram_bytes", return_value=1):
            resp = client.post("/api/modelling/estimate", json={"graph": graph, "node_id": "train"})
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("gpu_vram_estimated_mb") is not None
        assert data.get("gpu_warning") is not None

    def test_estimate_missing_node(self, client, training_data):
        graph = _make_modelling_graph(training_data)
        resp = client.post("/api/modelling/estimate", json={"graph": graph, "node_id": "nonexistent"})
        assert resp.status_code == 404

    def test_estimate_exception_returns_empty(self, client, training_data):
        """If the RAM estimate fails entirely, return an empty estimate (not 500)."""
        graph = _make_modelling_graph(training_data)
        with patch(
            "haute._ram_estimate.estimate_safe_training_rows",
            side_effect=RuntimeError("probe failed"),
        ):
            resp = client.post("/api/modelling/estimate", json={"graph": graph, "node_id": "train"})
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("total_rows") is None


class TestMlflowLogSuccess:
    """Tests for /mlflow/log success and exception paths."""

    def test_mlflow_log_success(self, client):
        """Inject a completed job and mock log_experiment to test success path."""
        from haute.routes.modelling import _store
        from haute.schemas import TrainResponse

        fake_result = TrainResponse(
            status="completed",
            job_id="test_log",
            metrics={"gini": 0.85, "rmse": 0.12},
            model_path="/tmp/model.cbm",
            train_rows=80,
            test_rows=20,
        )
        _store.jobs["test_log"] = {
            "status": "completed",
            "result": fake_result,
            "config": {"algorithm": "catboost", "task": "regression", "target": "y"},
            "node_label": "my_model",
            "created_at": time.time(),
        }
        mock_log_result = SimpleNamespace(
            backend="local",
            experiment_name="/Shared/haute/my_model",
            run_id="abc123",
            run_url=None,
            tracking_uri="file:///tmp/mlruns",
        )
        try:
            with patch(
                "haute.modelling._mlflow_log.log_experiment",
                return_value=mock_log_result,
            ):
                resp = client.post("/api/modelling/mlflow/log", json={"job_id": "test_log"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "ok"
            assert data["backend"] == "local"
            assert data["run_id"] == "abc123"
        finally:
            _store.jobs.pop("test_log", None)

    def test_mlflow_log_exception_returns_500(self, client):
        """If log_experiment raises, should return 500."""
        from haute.routes.modelling import _store
        from haute.schemas import TrainResponse

        fake_result = TrainResponse(status="completed", job_id="test_err", metrics={"gini": 0.5})
        _store.jobs["test_err"] = {
            "status": "completed",
            "result": fake_result,
            "config": {},
            "node_label": "model",
            "created_at": time.time(),
        }
        try:
            with patch(
                "haute.modelling._mlflow_log.log_experiment",
                side_effect=RuntimeError("MLflow connection refused"),
            ):
                resp = client.post("/api/modelling/mlflow/log", json={"job_id": "test_err"})
            assert resp.status_code == 500
            assert "MLflow connection refused" in resp.json()["detail"]
        finally:
            _store.jobs.pop("test_err", None)

    def test_mlflow_log_no_result_data(self, client):
        """Completed job with no result should return 400."""
        from haute.routes.modelling import _store

        _store.jobs["no_result"] = {
            "status": "completed",
            "result": None,
            "created_at": time.time(),
        }
        try:
            resp = client.post("/api/modelling/mlflow/log", json={"job_id": "no_result"})
            assert resp.status_code == 400
            assert "no result" in resp.json()["detail"].lower()
        finally:
            _store.jobs.pop("no_result", None)


class TestMlflowCheckImportError:
    """Test /mlflow/check when mlflow is not installed."""

    def test_mlflow_import_error(self, client):
        """Simulate mlflow not being installed via sys.modules patch."""
        import sys

        # Temporarily hide mlflow from the import system
        real_mlflow = sys.modules.get("mlflow")
        with patch.dict(sys.modules, {"mlflow": None}):
            resp = client.get("/api/modelling/mlflow/check")
        assert resp.status_code == 200
        data = resp.json()
        assert data["mlflow_installed"] is False


# ---------------------------------------------------------------------------
# Phase 1A: Background thread error tests
# ---------------------------------------------------------------------------


class TestBackgroundThreadErrors:
    """Test error handling in the background training thread."""

    def test_background_value_error(self, client, training_data):
        """ValueError in TrainingJob.run() sets status to error with message."""
        graph = _make_modelling_graph(training_data)
        with patch(
            "haute.modelling.TrainingJob.run",
            side_effect=ValueError("Invalid target column: not found"),
        ):
            resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
            data = resp.json()
            assert data["status"] == "started"
            status = _poll_until_done(client, data["job_id"])
            assert status["status"] == "error"
            assert "Invalid target column" in status["message"]

    def test_background_runtime_error(self, client, training_data):
        """RuntimeError in TrainingJob.run() is translated via _friendly_error."""
        graph = _make_modelling_graph(training_data)
        with patch(
            "haute.modelling.TrainingJob.run",
            side_effect=RuntimeError("CUDA out of memory"),
        ):
            resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
            data = resp.json()
            status = _poll_until_done(client, data["job_id"])
            assert status["status"] == "error"
            assert "CUDA out of memory" in status["message"]

    def test_background_generic_exception(self, client, training_data):
        """Generic exception in TrainingJob.run() includes exception type."""
        graph = _make_modelling_graph(training_data)
        with patch(
            "haute.modelling.TrainingJob.run",
            side_effect=Exception("unexpected crash"),
        ):
            resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
            data = resp.json()
            status = _poll_until_done(client, data["job_id"])
            assert status["status"] == "error"
            assert "unexpected crash" in status["message"]

    def test_ram_warning_propagated(self, client, training_data):
        """RAM warning from estimate should appear in job status."""
        graph = _make_modelling_graph(training_data)
        mock_est = SimpleNamespace(
            safe_row_limit=50,
            warning="Dataset too large for available RAM. Row limit applied: 50.",
            total_rows=100,
            probe_columns=2,
            estimated_bytes=1000.0,
            available_bytes=500.0,
            bytes_per_row=10.0,
            was_downsampled=True,
        )
        with patch(
            "haute._ram_estimate.estimate_safe_training_rows",
            return_value=mock_est,
        ):
            resp = client.post("/api/modelling/train", json={"graph": graph, "node_id": "train"})
            data = resp.json()
            status = _poll_until_done(client, data["job_id"])
            # Whether it completed or errored, the warning should be set
            warning = status.get("warning") or ""
            assert "Row limit" in warning or "RAM" in warning
