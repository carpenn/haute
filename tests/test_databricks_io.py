"""Tests for haute._databricks_io — credential resolution and fetch_and_cache.

The existing test_databricks_cache.py covers cache_path, cache_info,
clear_cache, fetch_progress, and read_cached_table.  This file focuses on
_get_credentials() and the fetch_and_cache() flow with a mock Databricks
connector.
"""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from haute._databricks_io import (
    CACHE_DIR,
    DatabricksConfigError,
    _clear_fetch_progress,
    _set_fetch_progress,
    fetch_and_cache,
    fetch_progress,
)

# ---------------------------------------------------------------------------
# _get_credentials
# ---------------------------------------------------------------------------


class TestGetCredentials:
    """Credential resolution from env vars and http_path argument."""

    def test_resolves_all_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "https://my-host.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-abc123")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")

        host, token, http_path = _get_credentials()
        assert host == "my-host.cloud.databricks.com"
        assert token == "dapi-abc123"
        assert http_path == "/sql/1.0/warehouses/abc"

    def test_http_path_arg_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "host.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/env-path")

        _, _, http_path = _get_credentials(http_path="/arg-path")
        assert http_path == "/arg-path"

    def test_strips_https_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "https://myhost.com/")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/path")

        host, _, _ = _get_credentials()
        assert host == "myhost.com"

    def test_strips_http_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "http://myhost.com/")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/path")

        host, _, _ = _get_credentials()
        assert host == "myhost.com"

    def test_strips_trailing_slash(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "myhost.com/")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        host, _, _ = _get_credentials(http_path="/p")
        assert host == "myhost.com"

    def test_raises_when_host_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        with pytest.raises(DatabricksConfigError, match="DATABRICKS_HOST"):
            _get_credentials(http_path="/p")

    def test_raises_when_token_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

        with pytest.raises(DatabricksConfigError, match="DATABRICKS_TOKEN"):
            _get_credentials(http_path="/p")

    def test_raises_when_http_path_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

        with pytest.raises(DatabricksConfigError, match="http_path"):
            _get_credentials()

    def test_raises_with_multiple_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)

        with pytest.raises(DatabricksConfigError) as exc_info:
            _get_credentials()
        msg = str(exc_info.value)
        assert "DATABRICKS_HOST" in msg
        assert "DATABRICKS_TOKEN" in msg
        assert "http_path" in msg

    def test_bare_host_no_protocol_passthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from haute._databricks_io import _get_credentials

        monkeypatch.setenv("DATABRICKS_HOST", "myhost.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        host, _, _ = _get_credentials(http_path="/p")
        assert host == "myhost.com"


# ---------------------------------------------------------------------------
# fetch_and_cache (with mocked Databricks connector)
# ---------------------------------------------------------------------------


def _make_arrow_batch(rows: int = 10) -> pa.Table:
    """Create a small PyArrow table to simulate a Databricks fetch batch."""
    return pa.table({
        "id": list(range(rows)),
        "value": [float(i) * 1.5 for i in range(rows)],
    })


def _empty_batch_like(batch: pa.Table) -> pa.Table:
    """Create an empty Arrow table with the same schema as *batch*."""
    return batch.schema.empty_table()


def _mock_dbsql_module(connect_fn: MagicMock) -> MagicMock:
    """Build a mock ``databricks.sql`` module with the given connect function.

    Because ``fetch_and_cache`` uses ``from databricks import sql as dbsql``
    as a local import, we must inject the mock into ``sys.modules`` so the
    import statement resolves to our mock.
    """
    mock_sql = MagicMock()
    mock_sql.connect = connect_fn
    mock_databricks = MagicMock()
    mock_databricks.sql = mock_sql
    return mock_databricks, mock_sql


def _build_mock_connector(batches: list[pa.Table]) -> tuple[MagicMock, MagicMock]:
    """Build a mock connector returning *batches* then an empty batch.

    Returns (mock_databricks_module, mock_sql_module) for injection
    into sys.modules.
    """
    empty = _empty_batch_like(batches[0])
    cursor = MagicMock()
    batch_iter = iter(batches + [empty])
    cursor.fetchmany_arrow = MagicMock(side_effect=batch_iter)

    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    mock_connect = MagicMock(return_value=conn)
    mock_databricks, mock_sql = _mock_dbsql_module(mock_connect)
    return mock_databricks, mock_sql, cursor


class TestFetchAndCache:
    """Tests for the full fetch_and_cache flow with mocked connector."""

    @pytest.fixture(autouse=True)
    def _cleanup_progress(self) -> None:
        yield
        _clear_fetch_progress()

    def test_writes_parquet_and_returns_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        batch = _make_arrow_batch(5)
        mock_db, mock_sql, _ = _build_mock_connector([batch])

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            result = fetch_and_cache(
                "catalog.schema.my_table",
                http_path="/sql/1.0/wh/abc",
                project_root=tmp_path,
            )

        assert result["table"] == "catalog.schema.my_table"
        assert result["row_count"] == 5
        assert result["column_count"] == 2
        assert result["fetch_seconds"] >= 0
        assert result["size_bytes"] > 0
        assert "id" in result["columns"]
        assert "value" in result["columns"]
        # File should exist on disk
        expected_path = tmp_path / CACHE_DIR / "catalog_schema_my_table.parquet"
        assert expected_path.exists()

    def test_rejects_invalid_table_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        with pytest.raises(ValueError, match="Invalid table name"):
            fetch_and_cache(
                "DROP TABLE students; --",
                http_path="/path",
                project_root=tmp_path,
            )

    def test_rejects_single_part_table_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        with pytest.raises(ValueError, match="Invalid table name"):
            fetch_and_cache(
                "just_a_table",
                http_path="/path",
                project_root=tmp_path,
            )

    def test_clears_progress_after_fetch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        batch = _make_arrow_batch(3)
        mock_db, mock_sql, _ = _build_mock_connector([batch])

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            fetch_and_cache(
                "cat.sch.tbl",
                http_path="/path",
                project_root=tmp_path,
            )

        # Progress should be cleared after successful fetch
        assert fetch_progress("cat.sch.tbl") is None

    def test_progress_cleared_on_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("connection failed")

        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        mock_connect = MagicMock(return_value=conn)
        mock_db, mock_sql = _mock_dbsql_module(mock_connect)

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            with pytest.raises(RuntimeError, match="connection failed"):
                fetch_and_cache(
                    "cat.sch.tbl",
                    http_path="/path",
                    project_root=tmp_path,
                )

        # Progress should be cleaned up even after error
        assert fetch_progress("cat.sch.tbl") is None

    def test_no_temp_file_on_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("boom")

        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)

        mock_connect = MagicMock(return_value=conn)
        mock_db, mock_sql = _mock_dbsql_module(mock_connect)

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            with pytest.raises(RuntimeError):
                fetch_and_cache(
                    "cat.sch.tbl",
                    http_path="/path",
                    project_root=tmp_path,
                )

        # Verify no temp files leaked — unconditional, not guarded by exists()
        assert not any(tmp_path.rglob("*.tmp")), (
            f"Temp file left behind: {list(tmp_path.rglob('*.tmp'))}"
        )

    def test_uses_custom_query(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        batch = _make_arrow_batch(2)
        mock_db, mock_sql, cursor = _build_mock_connector([batch])

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            fetch_and_cache(
                "cat.sch.tbl",
                http_path="/path",
                query="SELECT id, value",
                project_root=tmp_path,
            )

        # Verify the SQL query passed to execute
        cursor.execute.assert_called_once()
        executed_sql = cursor.execute.call_args[0][0]
        assert "SELECT id, value FROM cat.sch.tbl" == executed_sql

    def test_multiple_batches(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "host.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        batch1 = _make_arrow_batch(3)
        batch2 = _make_arrow_batch(4)
        mock_db, mock_sql, _ = _build_mock_connector([batch1, batch2])

        with patch.dict("sys.modules", {"databricks": mock_db, "databricks.sql": mock_sql}):
            result = fetch_and_cache(
                "cat.sch.tbl",
                http_path="/path",
                project_root=tmp_path,
            )

        assert result["row_count"] == 7  # 3 + 4


# ---------------------------------------------------------------------------
# Thread-safety of fetch progress helpers
# ---------------------------------------------------------------------------


class TestFetchProgressThreadSafety:
    """Verify concurrent progress updates don't lose data."""

    @pytest.fixture(autouse=True)
    def _cleanup_progress(self) -> None:
        yield
        _clear_fetch_progress()

    def test_concurrent_set_and_read(self) -> None:
        errors: list[str] = []
        iterations = 100

        def writer(table: str) -> None:
            for i in range(iterations):
                _set_fetch_progress(table, {"rows": i})

        def reader(table: str) -> None:
            for _ in range(iterations):
                progress = fetch_progress(table)
                if progress is not None:
                    if "rows" not in progress:
                        errors.append(f"Missing 'rows' key in {progress}")

        t1 = threading.Thread(target=writer, args=("t1",))
        t2 = threading.Thread(target=reader, args=("t1",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == [], f"Thread safety errors: {errors}"
