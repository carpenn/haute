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

        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
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

        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
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

        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
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

        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
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


# ---------------------------------------------------------------------------
# GET /api/databricks/cache
# ---------------------------------------------------------------------------


class TestCacheStatus:
    def test_not_cached(self, client: TestClient) -> None:
        resp = client.get("/api/databricks/cache", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["cached"] is False
        assert data["table"] == "cat.sch.tbl"

    def test_cached_after_write(self, client: TestClient) -> None:
        import polars as pl

        from haute._databricks_io import _cache_path_for

        p = _cache_path_for("cat.sch.tbl")  # uses Path.cwd() set by client fixture
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)

        resp = client.get("/api/databricks/cache", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["cached"] is True
        assert data["row_count"] == 3
        assert data["column_count"] == 1
        assert data["size_bytes"] > 0

    def test_delete_cache(self, client: TestClient) -> None:
        import polars as pl

        from haute._databricks_io import _cache_path_for

        p = _cache_path_for("cat.sch.tbl")
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1]}).write_parquet(p)
        assert p.exists()

        resp = client.delete("/api/databricks/cache", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["cached"] is False
        assert not p.exists()

    def test_delete_cache_noop_when_missing(self, client: TestClient) -> None:
        resp = client.delete("/api/databricks/cache", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        assert resp.json()["cached"] is False


# ---------------------------------------------------------------------------
# GET /api/databricks/fetch/progress
# ---------------------------------------------------------------------------


class TestFetchProgress:
    def test_no_active_fetch(self, client: TestClient) -> None:
        resp = client.get("/api/databricks/fetch/progress", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        assert resp.json()["active"] is False

    def test_active_fetch(self, client: TestClient) -> None:
        from haute._databricks_io import _fetch_lock, _fetch_progress

        with _fetch_lock:
            _fetch_progress["cat.sch.tbl"] = {"rows": 200_000, "batches": 2, "elapsed": 3.5}
        try:
            resp = client.get("/api/databricks/fetch/progress", params={"table": "cat.sch.tbl"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["active"] is True
            assert data["rows"] == 200_000
            assert data["batches"] == 2
            assert data["elapsed"] == 3.5
        finally:
            with _fetch_lock:
                _fetch_progress.pop("cat.sch.tbl", None)


# ---------------------------------------------------------------------------
# POST /api/databricks/fetch
# ---------------------------------------------------------------------------


class TestFetchTable:
    def test_fetch_success(self, client: TestClient) -> None:
        from haute._databricks_io import _cache_path_for

        fake_result = {
            "path": str(_cache_path_for("cat.sch.tbl")),
            "table": "cat.sch.tbl",
            "row_count": 100,
            "column_count": 3,
            "columns": {"a": "Int64", "b": "Utf8", "c": "Float64"},
            "size_bytes": 4096,
            "fetched_at": 1700000000.0,
            "fetch_seconds": 1.5,
        }

        with patch("haute._databricks_io.fetch_and_cache", return_value=fake_result):
            resp = client.post("/api/databricks/fetch", json={
                "table": "cat.sch.tbl",
                "http_path": "/sql/wh",
            })

        assert resp.status_code == 200
        data = resp.json()
        assert data["table"] == "cat.sch.tbl"
        assert data["row_count"] == 100
        assert data["column_count"] == 3
        assert data["size_bytes"] == 4096
        assert data["fetch_seconds"] == 1.5

    def test_fetch_missing_connector_returns_400(self, client: TestClient) -> None:
        with patch("haute._databricks_io.fetch_and_cache", side_effect=ImportError("no module")):
            resp = client.post("/api/databricks/fetch", json={"table": "cat.sch.tbl"})
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GET /api/schema/databricks
# ---------------------------------------------------------------------------


class TestDatabricksSchema:
    def test_not_cached_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/schema/databricks", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 404

    def test_cached_returns_schema(self, client: TestClient) -> None:
        import polars as pl

        from haute._databricks_io import _cache_path_for

        p = _cache_path_for("cat.sch.tbl")  # uses Path.cwd() set by client fixture
        p.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1, 2, 3], "y": [4.0, 5.0, 6.0]}).write_parquet(p)

        resp = client.get("/api/schema/databricks", params={"table": "cat.sch.tbl"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert data["column_count"] == 2
        assert len(data["preview"]) <= 5
