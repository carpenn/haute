"""Tests for Databricks browsing API endpoints (mocked — no real Databricks connection)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_test_token")
    (tmp_path / "main.py").write_text("")
    from haute.server import app

    return TestClient(app)


# ---------------------------------------------------------------------------
# GET /api/databricks/warehouses
# ---------------------------------------------------------------------------


class TestListWarehouses:
    def test_returns_warehouse_list(self, client: TestClient) -> None:
        mock_wh = MagicMock()
        mock_wh.id = "abc123"
        mock_wh.name = "Starter Warehouse"
        mock_wh.state = MagicMock(value="RUNNING")
        mock_wh.cluster_size = "Small"

        mock_ws = MagicMock()
        mock_ws.warehouses.list.return_value = [mock_wh]

        with patch("haute.server._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/warehouses")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["warehouses"]) == 1
        wh = data["warehouses"][0]
        assert wh["id"] == "abc123"
        assert wh["name"] == "Starter Warehouse"
        assert wh["http_path"] == "/sql/1.0/warehouses/abc123"
        assert wh["state"] == "RUNNING"
        assert wh["size"] == "Small"

    def test_missing_credentials_returns_400(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_DATA_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_DATA_TOKEN", raising=False)
        from haute.server import app

        c = TestClient(app)
        resp = c.get("/api/databricks/warehouses")
        assert resp.status_code == 400
        assert "DATABRICKS_HOST" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/databricks/catalogs
# ---------------------------------------------------------------------------


class TestListCatalogs:
    def test_returns_catalog_list(self, client: TestClient) -> None:
        mock_cat = MagicMock()
        mock_cat.name = "main"
        mock_cat.comment = "Default catalog"

        mock_ws = MagicMock()
        mock_ws.catalogs.list.return_value = [mock_cat]

        with patch("haute.server._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/catalogs")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["catalogs"]) == 1
        assert data["catalogs"][0]["name"] == "main"
        assert data["catalogs"][0]["comment"] == "Default catalog"


# ---------------------------------------------------------------------------
# GET /api/databricks/schemas
# ---------------------------------------------------------------------------


class TestListSchemas:
    def test_returns_schema_list(self, client: TestClient) -> None:
        mock_sch = MagicMock()
        mock_sch.name = "pricing"
        mock_sch.comment = ""

        mock_ws = MagicMock()
        mock_ws.schemas.list.return_value = [mock_sch]

        with patch("haute.server._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/schemas", params={"catalog": "main"})

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["schemas"]) == 1
        assert data["schemas"][0]["name"] == "pricing"

    def test_missing_catalog_param_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/databricks/schemas")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/databricks/tables
# ---------------------------------------------------------------------------


class TestListTables:
    def test_returns_table_list(self, client: TestClient) -> None:
        mock_tbl = MagicMock()
        mock_tbl.name = "policies"
        mock_tbl.full_name = "main.pricing.policies"
        mock_tbl.table_type = MagicMock(value="MANAGED")
        mock_tbl.comment = "Policy data"

        mock_ws = MagicMock()
        mock_ws.tables.list.return_value = [mock_tbl]

        with patch("haute.server._get_databricks_client", return_value=mock_ws):
            resp = client.get(
                "/api/databricks/tables",
                params={"catalog": "main", "schema": "pricing"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["tables"]) == 1
        tbl = data["tables"][0]
        assert tbl["name"] == "policies"
        assert tbl["full_name"] == "main.pricing.policies"
        assert tbl["table_type"] == "MANAGED"
        assert tbl["comment"] == "Policy data"

    def test_missing_params_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/databricks/tables", params={"catalog": "main"})
        assert resp.status_code == 422
