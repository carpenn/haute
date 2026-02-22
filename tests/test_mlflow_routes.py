"""API integration tests for MLflow discovery endpoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from haute.server import app


@pytest.fixture()
def client():
    return TestClient(app)


def _mock_tracking(mlflow=None, client=None):
    """Patch ``_ensure_tracking`` to return ``(mlflow, client)``."""
    mlflow = mlflow or MagicMock()
    client = client or MagicMock()
    return patch("haute.routes.mlflow._ensure_tracking", return_value=(mlflow, client))


# ---------------------------------------------------------------------------
# GET /api/mlflow/experiments
# ---------------------------------------------------------------------------


class TestListExperiments:
    def test_list_experiments(self, client):
        """Returns list of experiments from MLflow."""
        class FakeExp:
            experiment_id = "1"
            name = "test-exp"

        mock_mlflow = MagicMock()
        mock_mlflow.search_experiments.return_value = [FakeExp()]

        with _mock_tracking(mlflow=mock_mlflow):
            resp = client.get("/api/mlflow/experiments")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["experiment_id"] == "1"
        assert data[0]["name"] == "test-exp"

    def test_mlflow_not_installed_503(self, client):
        """Returns 503 when mlflow is not installed."""
        with patch(
            "haute.routes.mlflow._ensure_tracking",
            side_effect=HTTPException(status_code=503, detail="not installed"),
        ):
            resp = client.get("/api/mlflow/experiments")

        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# GET /api/mlflow/runs
# ---------------------------------------------------------------------------


class TestListRuns:
    def test_list_runs_filters_cbm(self, client):
        """Only returns runs with .cbm artifacts."""
        run1 = MagicMock()
        run1.info.run_id = "run1"
        run1.info.run_name = "good-run"
        run1.info.status = "FINISHED"
        run1.info.start_time = 1000
        run1.data.metrics = {"rmse": 0.5}
        run1.data.params = {"lr": "0.05"}

        cbm_art = MagicMock(path="model.cbm")
        mock_mlflow = MagicMock()
        mock_mlflow.search_runs.return_value = [run1]
        mock_client = MagicMock()
        mock_client.list_artifacts.return_value = [cbm_art]

        with _mock_tracking(mlflow=mock_mlflow, client=mock_client):
            resp = client.get("/api/mlflow/runs?experiment_id=1")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["run_id"] == "run1"
        assert "model.cbm" in data[0]["artifacts"]

    def test_runs_without_cbm_excluded(self, client):
        """Runs without .cbm artifacts are excluded."""
        run1 = MagicMock()
        run1.info.run_id = "run1"
        run1.info.run_name = "no-model"
        run1.info.status = "FINISHED"
        run1.info.start_time = 1000
        run1.data.metrics = {}
        run1.data.params = {}

        txt_art = MagicMock(path="readme.txt")
        mock_mlflow = MagicMock()
        mock_mlflow.search_runs.return_value = [run1]
        mock_client = MagicMock()
        mock_client.list_artifacts.return_value = [txt_art]

        with _mock_tracking(mlflow=mock_mlflow, client=mock_client):
            resp = client.get("/api/mlflow/runs?experiment_id=1")

        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# GET /api/mlflow/models
# ---------------------------------------------------------------------------


class TestListModels:
    def test_list_models(self, client):
        """Returns list of registered models."""
        model = MagicMock()
        model.name = "my-model"
        v = MagicMock(version="1", status="READY", run_id="run1")
        model.latest_versions = [v]

        mock_client = MagicMock()
        mock_client.search_registered_models.return_value = [model]

        with _mock_tracking(client=mock_client):
            resp = client.get("/api/mlflow/models")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "my-model"
        assert data[0]["latest_versions"][0]["version"] == "1"


# ---------------------------------------------------------------------------
# GET /api/mlflow/model-versions
# ---------------------------------------------------------------------------


class TestListModelVersions:
    def test_list_model_versions(self, client):
        """Returns sorted versions of a model."""
        v1 = MagicMock(version="1", run_id="r1", status="READY", creation_timestamp=100, description="first")
        v2 = MagicMock(version="2", run_id="r2", status="READY", creation_timestamp=200, description="second")

        mock_client = MagicMock()
        mock_client.search_model_versions.return_value = [v1, v2]

        with _mock_tracking(client=mock_client):
            resp = client.get("/api/mlflow/model-versions?model_name=my-model")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        # Should be sorted descending by version
        assert data[0]["version"] == "2"
        assert data[1]["version"] == "1"

    def test_missing_model_name_422(self, client):
        """Returns 422 when model_name query param is missing."""
        resp = client.get("/api/mlflow/model-versions")
        assert resp.status_code == 422

    def test_pagination_page_token(self, client):
        """page_token is forwarded to MLflow client."""
        mock_client = MagicMock()
        mock_client.search_registered_models.return_value = []

        with _mock_tracking(client=mock_client):
            resp = client.get("/api/mlflow/models?max_results=10&page_token=abc123")

        assert resp.status_code == 200
        mock_client.search_registered_models.assert_called_once_with(
            max_results=10, page_token="abc123",
        )
