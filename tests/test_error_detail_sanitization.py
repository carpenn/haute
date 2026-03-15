"""Tests for E3 and E8 error handling fixes.

E3: Verify no route leaks raw Python exception messages via HTTP 500
    ``detail`` strings. All routes must return a safe, generic message
    and log the actual error server-side.

E8: Verify ``_execute_eager_core`` logs node failures at ``error`` level,
    not ``warning``.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from fastapi.testclient import TestClient


# ── Shared safe-detail constant ──────────────────────────────────────────

_SAFE_DETAIL = "Operation failed. Check the server logs for details."


# ── Fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture()
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with cwd set to a temp directory and Databricks env set."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi_test_token")
    (tmp_path / "main.py").write_text("")
    from haute.server import app

    return TestClient(app)


# =====================================================================
# E3: Route-level error detail sanitization
# =====================================================================


class TestDatabricksRoutesSafeDetail:
    """All Databricks routes must return safe detail on 500."""

    def test_warehouses_500_no_leak(self, client: TestClient) -> None:
        mock_ws = MagicMock()
        mock_ws.warehouses.list.side_effect = RuntimeError(
            "/home/user/.databricks/token: permission denied"
        )
        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/warehouses")
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "permission denied" not in detail
        assert ".databricks/token" not in detail
        assert detail == _SAFE_DETAIL

    def test_catalogs_500_no_leak(self, client: TestClient) -> None:
        mock_ws = MagicMock()
        mock_ws.catalogs.list.side_effect = RuntimeError(
            "AuthenticationError: invalid token xyz-secret"
        )
        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/catalogs")
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "xyz-secret" not in detail
        assert detail == _SAFE_DETAIL

    def test_schemas_500_no_leak(self, client: TestClient) -> None:
        mock_ws = MagicMock()
        mock_ws.schemas.list.side_effect = RuntimeError(
            "SDK internal: /var/run/secrets/token read failure"
        )
        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
            resp = client.get("/api/databricks/schemas", params={"catalog": "main"})
        assert resp.status_code == 500
        assert resp.json()["detail"] == _SAFE_DETAIL

    def test_tables_500_no_leak(self, client: TestClient) -> None:
        mock_ws = MagicMock()
        mock_ws.tables.list.side_effect = RuntimeError(
            "Connection to host 10.0.0.5:443 refused"
        )
        with patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws):
            resp = client.get(
                "/api/databricks/tables",
                params={"catalog": "cat", "schema": "sch"},
            )
        assert resp.status_code == 500
        assert "10.0.0.5" not in resp.json()["detail"]
        assert resp.json()["detail"] == _SAFE_DETAIL

    def test_fetch_500_no_leak(self, client: TestClient) -> None:
        with patch(
            "haute._databricks_io.fetch_and_cache",
            side_effect=RuntimeError("OSError: /mnt/data/cache full"),
        ):
            resp = client.post(
                "/api/databricks/fetch",
                json={"table": "cat.sch.tbl", "http_path": "/sql/wh"},
            )
        assert resp.status_code == 500
        assert "/mnt/data/cache" not in resp.json()["detail"]
        assert resp.json()["detail"] == _SAFE_DETAIL


class TestDatabricksRoutesLogOnError:
    """Verify errors are logged at error level for each Databricks endpoint."""

    def test_warehouses_logs_error(self, client: TestClient) -> None:
        mock_ws = MagicMock()
        mock_ws.warehouses.list.side_effect = RuntimeError("secret-err")
        mock_logger = MagicMock()
        with (
            patch("haute.routes.databricks._get_databricks_client", return_value=mock_ws),
            patch("haute.routes.databricks.logger", mock_logger),
        ):
            client.get("/api/databricks/warehouses")
        mock_logger.error.assert_called_once()
        call_kwargs = mock_logger.error.call_args
        assert "secret-err" in str(call_kwargs)

    def test_fetch_logs_error(self, client: TestClient) -> None:
        mock_logger = MagicMock()
        with (
            patch(
                "haute._databricks_io.fetch_and_cache",
                side_effect=RuntimeError("internal-boom"),
            ),
            patch("haute.routes.databricks.logger", mock_logger),
        ):
            client.post(
                "/api/databricks/fetch",
                json={"table": "cat.sch.tbl", "http_path": "/sql/wh"},
            )
        mock_logger.error.assert_called_once()
        call_kwargs = mock_logger.error.call_args
        assert "internal-boom" in str(call_kwargs)


class TestPipelineRoutesSafeDetail:
    """Pipeline trace/preview/sink must return safe detail on 500."""

    @pytest.fixture()
    def pipeline_graph(self, tmp_path: Path):
        """Create a minimal pipeline file and return its parsed graph."""
        from haute.parser import parse_pipeline_file

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        data_path = data_dir / "input.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(data_path)

        code = f"""\
import haute
pipeline = haute.Pipeline("test")

@pipeline.node(type="dataSource", path="{data_path}")
def source(config):
    pass
"""
        py = tmp_path / "test_pipeline.py"
        py.write_text(code)
        return parse_pipeline_file(py)

    def test_trace_500_no_leak(self, client: TestClient, pipeline_graph) -> None:
        with patch(
            "haute.trace.execute_trace",
            side_effect=RuntimeError("traceback: File /home/user/secret.py line 42"),
        ):
            resp = client.post(
                "/api/pipeline/trace",
                json={"graph": pipeline_graph.model_dump(), "row_index": 0},
            )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "/home/user/secret.py" not in detail
        assert detail == _SAFE_DETAIL

    def test_preview_500_no_leak(self, client: TestClient, pipeline_graph) -> None:
        node_id = pipeline_graph.nodes[0].id
        with patch(
            "haute.executor.execute_graph",
            side_effect=RuntimeError("MemoryError at 0x7fff5e3a1000"),
        ):
            resp = client.post(
                "/api/pipeline/preview",
                json={"graph": pipeline_graph.model_dump(), "node_id": node_id},
            )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "0x7fff" not in detail
        assert detail == _SAFE_DETAIL

    def test_sink_500_no_leak(self, client: TestClient, tmp_path: Path) -> None:
        data_path = tmp_path / "data" / "input.parquet"
        graph = {
            "nodes": [
                {
                    "id": "src",
                    "type": "pipelineNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "src",
                        "nodeType": "dataSource",
                        "config": {"path": str(data_path)},
                    },
                },
                {
                    "id": "sink",
                    "type": "pipelineNode",
                    "position": {"x": 300, "y": 0},
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": "/tmp/test_sink.parquet", "format": "parquet"},
                    },
                },
            ],
            "edges": [{"id": "e1", "source": "src", "target": "sink"}],
        }
        with patch(
            "haute.executor.execute_sink",
            side_effect=RuntimeError("PermissionError: /secure/dir/output.parquet"),
        ):
            resp = client.post(
                "/api/pipeline/sink", json={"graph": graph, "node_id": "sink"}
            )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "/secure/dir/" not in detail
        assert detail == _SAFE_DETAIL


class TestPipelineRoutesLogOnError:
    """Verify pipeline routes log the actual error at ERROR level."""

    @pytest.fixture()
    def pipeline_graph(self, tmp_path: Path):
        from haute.parser import parse_pipeline_file

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        data_path = data_dir / "input.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(data_path)

        code = f"""\
import haute
pipeline = haute.Pipeline("test")

@pipeline.node(type="dataSource", path="{data_path}")
def source(config):
    pass
"""
        py = tmp_path / "test_pipeline.py"
        py.write_text(code)
        return parse_pipeline_file(py)

    def test_trace_logs_error(self, client: TestClient, pipeline_graph) -> None:
        mock_logger = MagicMock()
        with (
            patch(
                "haute.trace.execute_trace",
                side_effect=RuntimeError("real-trace-error"),
            ),
            patch("haute.routes.pipeline.logger", mock_logger),
        ):
            client.post(
                "/api/pipeline/trace",
                json={"graph": pipeline_graph.model_dump(), "row_index": 0},
            )
        mock_logger.error.assert_called()
        assert "real-trace-error" in str(mock_logger.error.call_args)

    def test_preview_logs_error(self, client: TestClient, pipeline_graph) -> None:
        mock_logger = MagicMock()
        node_id = pipeline_graph.nodes[0].id
        with (
            patch(
                "haute.executor.execute_graph",
                side_effect=RuntimeError("real-preview-error"),
            ),
            patch("haute.routes.pipeline.logger", mock_logger),
        ):
            client.post(
                "/api/pipeline/preview",
                json={"graph": pipeline_graph.model_dump(), "node_id": node_id},
            )
        mock_logger.error.assert_called()
        assert "real-preview-error" in str(mock_logger.error.call_args)

    def test_sink_logs_error(self, client: TestClient) -> None:
        mock_logger = MagicMock()
        graph = {
            "nodes": [
                {
                    "id": "src",
                    "type": "pipelineNode",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "src",
                        "nodeType": "dataSource",
                        "config": {"path": "/tmp/fake.parquet"},
                    },
                },
            ],
            "edges": [],
        }
        with (
            patch(
                "haute.executor.execute_sink",
                side_effect=RuntimeError("real-sink-error"),
            ),
            patch("haute.routes.pipeline.logger", mock_logger),
        ):
            client.post(
                "/api/pipeline/sink", json={"graph": graph, "node_id": "src"}
            )
        mock_logger.error.assert_called()
        assert "real-sink-error" in str(mock_logger.error.call_args)


class TestJsonCacheRoutesSafeDetail:
    """JSON cache build 500 must not leak internal details."""

    def test_build_500_no_leak(self, client: TestClient) -> None:
        with patch(
            "haute._json_flatten.build_json_cache",
            side_effect=RuntimeError("OSError: [Errno 28] No space left on device: '/tmp/x'"),
        ):
            resp = client.post("/api/json-cache/build", json={"path": "data.jsonl"})
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "/tmp/x" not in detail
        assert detail == _SAFE_DETAIL

    def test_build_logs_error(self, client: TestClient) -> None:
        mock_logger = MagicMock()
        with (
            patch(
                "haute._json_flatten.build_json_cache",
                side_effect=RuntimeError("internal-json-error"),
            ),
            patch("haute.routes.json_cache.logger", mock_logger),
        ):
            client.post("/api/json-cache/build", json={"path": "data.jsonl"})
        mock_logger.error.assert_called_once()
        assert "internal-json-error" in str(mock_logger.error.call_args)


class TestOptimiserRoutesSafeDetail:
    """Optimiser apply/frontier/save/mlflow routes must not leak details."""

    @pytest.fixture()
    def clean_job_store(self):
        from haute.routes.optimiser import _store

        snapshot = dict(_store.jobs)
        yield _store
        _store.jobs.clear()
        _store.jobs.update(snapshot)

    def test_apply_500_no_leak(self, client: TestClient, clean_job_store) -> None:
        """apply_lambdas must return safe detail on internal error."""
        store = clean_job_store
        mock_solve_result = MagicMock()
        mock_solve_result.dataframe = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("numpy internal: segfault at 0xdead"))
        )
        # Make .dataframe raise on access
        type(mock_solve_result).dataframe = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("numpy internal: segfault at 0xdead"))
        )
        store.jobs["test_apply_err"] = {
            "status": "completed",
            "solve_result": mock_solve_result,
            "created_at": time.time(),
        }
        resp = client.post(
            "/api/optimiser/apply", json={"job_id": "test_apply_err"}
        )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "segfault" not in detail
        assert "0xdead" not in detail
        assert detail == _SAFE_DETAIL

    def test_frontier_500_no_leak(self, client: TestClient, clean_job_store) -> None:
        """run_frontier must return safe detail on internal error."""
        store = clean_job_store
        mock_solver = MagicMock()
        mock_solver.frontier.side_effect = RuntimeError(
            "Rust panic: thread 'solver' panicked at core/src/lib.rs:42"
        )
        store.jobs["test_frontier_err"] = {
            "status": "completed",
            "solver": mock_solver,
            "quote_grid": MagicMock(),
            "created_at": time.time(),
        }
        resp = client.post(
            "/api/optimiser/frontier",
            json={
                "job_id": "test_frontier_err",
                "threshold_ranges": {"volume": [0.9, 1.1]},
            },
        )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "lib.rs:42" not in detail
        assert detail == _SAFE_DETAIL

    def test_save_oserror_no_path_leak(self, client: TestClient, clean_job_store, tmp_path, monkeypatch) -> None:
        """save_result OSError must not leak filesystem paths."""
        from haute._sandbox import set_project_root

        set_project_root(tmp_path)
        store = clean_job_store
        mock_solve_result = SimpleNamespace(
            lambdas={"x": 1.0},
            total_objective=100.0,
            total_constraints={"vol": 0.5},
            converged=True,
        )
        store.jobs["test_save_err"] = {
            "status": "completed",
            "solve_result": mock_solve_result,
            "solver": MagicMock(),
            "config": {},
            "created_at": time.time(),
        }
        with patch("pathlib.Path.write_text", side_effect=OSError(
            "Permission denied: '/secure/results/output.json'"
        )):
            resp = client.post(
                "/api/optimiser/save",
                json={
                    "job_id": "test_save_err",
                    "output_path": "results/output.json",
                },
            )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "/secure/results/" not in detail
        assert "Check the server logs" in detail

    def test_mlflow_log_500_no_leak(self, client: TestClient, clean_job_store) -> None:
        """mlflow_log must return safe detail on internal error."""
        store = clean_job_store
        mock_solver = MagicMock()
        mock_solve_result = MagicMock()
        store.jobs["test_mlflow_err"] = {
            "status": "completed",
            "solver": mock_solver,
            "solve_result": mock_solve_result,
            "config": {},
            "node_label": "opt",
            "created_at": time.time(),
        }
        with patch.dict("sys.modules", {"mlflow": MagicMock()}):
            with patch(
                "haute.modelling._mlflow_log.resolve_tracking_backend",
                side_effect=RuntimeError("ConnectionError: https://internal-mlflow.corp:5000 refused"),
            ):
                resp = client.post(
                    "/api/optimiser/mlflow/log",
                    json={"job_id": "test_mlflow_err"},
                )
        assert resp.status_code == 500
        detail = resp.json()["detail"]
        assert "internal-mlflow.corp" not in detail
        assert detail == _SAFE_DETAIL


class TestModellingRoutesSafeDetail:
    """Modelling mlflow log must not leak details."""

    def test_mlflow_log_500_no_leak(self, client: TestClient) -> None:
        from haute.routes.modelling import _store

        _store.jobs["test_err"] = {
            "status": "completed",
            "result": SimpleNamespace(
                metrics={},
                model_path=None,
                diagnostics={},
                metadata={},
            ),
            "config": {},
            "node_label": "model",
            "created_at": time.time(),
        }
        try:
            with patch(
                "haute.modelling._mlflow_log.log_experiment",
                side_effect=RuntimeError("ODBC driver not found at /opt/simba/lib"),
            ):
                resp = client.post(
                    "/api/modelling/mlflow/log",
                    json={"job_id": "test_err"},
                )
            assert resp.status_code == 500
            detail = resp.json()["detail"]
            assert "/opt/simba/lib" not in detail
            assert detail == _SAFE_DETAIL
        finally:
            _store.jobs.pop("test_err", None)


# =====================================================================
# E3: Verify intentional domain errors still pass through
# =====================================================================


class TestDomainErrorsStillExposed:
    """400-level errors with domain messages must still be exposed to users."""

    def test_git_guardrail_error_exposed(self, client: TestClient) -> None:
        """GitGuardrailError detail is intentional and user-facing."""
        from haute._git import GitGuardrailError

        with patch(
            "haute.routes.git.get_status",
            side_effect=GitGuardrailError("Cannot push to protected branch 'main'"),
        ):
            resp = client.get("/api/git/status")
        assert resp.status_code == 403
        assert "Cannot push to protected branch" in resp.json()["detail"]

    def test_git_error_exposed(self, client: TestClient) -> None:
        """GitError detail is intentional and user-facing."""
        from haute._git import GitError

        with patch(
            "haute.routes.git.get_status",
            side_effect=GitError("No git repository found"),
        ):
            resp = client.get("/api/git/status")
        assert resp.status_code == 400
        assert "No git repository found" in resp.json()["detail"]

    def test_file_schema_value_error_exposed(self, client: TestClient, tmp_path: Path) -> None:
        """ValueError on schema read is a user-facing validation error (400)."""
        target = tmp_path / "bad.csv"
        target.write_text("invalid")
        with patch(
            "haute.graph_utils.read_source",
            side_effect=ValueError("Unsupported file format: .xyz"),
        ):
            resp = client.get("/api/schema", params={"path": str(target)})
        assert resp.status_code == 400
        assert "Unsupported file format" in resp.json()["detail"]

    def test_databricks_missing_credentials_exposed(self, tmp_path, monkeypatch) -> None:
        """Missing credentials is user-facing (400), not a leak."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        from haute.server import app

        c = TestClient(app)
        resp = c.get("/api/databricks/warehouses")
        assert resp.status_code == 400
        assert "DATABRICKS_HOST" in resp.json()["detail"]


# =====================================================================
# E3: Constant consistency check
# =====================================================================


class TestInternalErrorDetailConstant:
    """Verify that each route module defines the safe error constant."""

    @pytest.mark.parametrize("module_path", [
        "haute.routes.databricks",
        "haute.routes.pipeline",
        "haute.routes.json_cache",
        "haute.routes.optimiser",
        "haute.routes.modelling",
        "haute.routes.git",
        "haute.routes.mlflow",
    ])
    def test_module_has_internal_error_detail(self, module_path: str) -> None:
        import importlib

        mod = importlib.import_module(module_path)
        assert hasattr(mod, "_INTERNAL_ERROR_DETAIL")
        assert "Check the server logs" in mod._INTERNAL_ERROR_DETAIL


# =====================================================================
# E8: Node execution failure log level
# =====================================================================


class TestNodeFailureLogLevel:
    """Verify that _execute_eager_core logs node failures at ERROR, not WARNING."""

    def test_node_failure_logged_at_error_level(self) -> None:
        """When swallow_errors=True, node failures must be logged at ERROR level."""
        from haute._execute_lazy import _execute_eager_core
        from haute._types import (
            GraphEdge,
            GraphNode,
            NodeData,
            NodeType,
            PipelineGraph,
        )

        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def failing_fn(*dfs):
                raise RuntimeError("test node failure")

            return node.id, failing_fn, False

        g = PipelineGraph(
            nodes=[
                GraphNode(id="src", data=NodeData(label="src", nodeType=NodeType.DATA_SOURCE)),
                GraphNode(id="t", data=NodeData(label="t", nodeType=NodeType.TRANSFORM)),
            ],
            edges=[GraphEdge(id="e_src_t", source="src", target="t")],
        )

        mock_logger = MagicMock()
        with patch("haute._execute_lazy.logger", mock_logger):
            result = _execute_eager_core(g, build_fn, swallow_errors=True)

        assert "t" in result.errors
        assert "test node failure" in result.errors["t"]

        # Verify logger.error was called (not logger.warning)
        mock_logger.error.assert_called_once()
        call_args = mock_logger.error.call_args
        assert call_args[0][0] == "node_failed"
        assert call_args[1]["node_id"] == "t"
        assert "test node failure" in call_args[1]["error"]

        # Verify logger.warning was NOT called for node failure
        mock_logger.warning.assert_not_called()

    def test_node_failure_not_logged_at_warning(self) -> None:
        """Confirm node failures are NOT logged at WARNING level anymore."""
        from haute._execute_lazy import _execute_eager_core
        from haute._types import (
            GraphEdge,
            GraphNode,
            NodeData,
            NodeType,
            PipelineGraph,
        )

        def build_fn(node, **kwargs):
            if node.data.nodeType == NodeType.DATA_SOURCE:
                return node.id, lambda: pl.DataFrame({"x": [1]}).lazy(), True

            def failing_fn(*dfs):
                raise RuntimeError("another failure")

            return node.id, failing_fn, False

        g = PipelineGraph(
            nodes=[
                GraphNode(id="src", data=NodeData(label="src", nodeType=NodeType.DATA_SOURCE)),
                GraphNode(id="t", data=NodeData(label="t", nodeType=NodeType.TRANSFORM)),
            ],
            edges=[GraphEdge(id="e_src_t", source="src", target="t")],
        )

        mock_logger = MagicMock()
        with patch("haute._execute_lazy.logger", mock_logger):
            _execute_eager_core(g, build_fn, swallow_errors=True)

        # logger.warning must never be called for node_failed
        for call in mock_logger.warning.call_args_list:
            assert "node_failed" not in str(call), (
                "node_failed should no longer be logged at WARNING level"
            )


# =====================================================================
# E3: MLflow discovery routes (mlflow.py) — 502 errors must not leak
# =====================================================================


class TestMlflowRoutesSafeDetail:
    """MLflow discovery routes must not leak raw exceptions in 502 responses."""

    def test_experiments_502_no_leak(self, client: TestClient) -> None:
        mock_mlflow = MagicMock()
        mock_mlflow.search_experiments.side_effect = RuntimeError(
            "ConnectionError: https://internal-mlflow.corp:5000/api refused"
        )
        with (
            patch("haute.routes.mlflow._ensure_tracking", return_value=(mock_mlflow, MagicMock())),
        ):
            resp = client.get("/api/mlflow/experiments")
        assert resp.status_code == 502
        detail = resp.json()["detail"]
        assert "internal-mlflow.corp" not in detail
        assert "Check the server logs" in detail

    def test_runs_502_no_leak(self, client: TestClient) -> None:
        mock_mlflow = MagicMock()
        mock_mlflow.search_runs.side_effect = RuntimeError(
            "SSLError: certificate verify failed for host mlflow.internal"
        )
        with (
            patch("haute.routes.mlflow._ensure_tracking", return_value=(mock_mlflow, MagicMock())),
        ):
            resp = client.get("/api/mlflow/runs", params={"experiment_id": "1"})
        assert resp.status_code == 502
        detail = resp.json()["detail"]
        assert "mlflow.internal" not in detail
        assert "Check the server logs" in detail

    def test_models_502_no_leak(self, client: TestClient) -> None:
        mock_client = MagicMock()
        mock_client.search_registered_models.side_effect = RuntimeError(
            "PermissionDenied: access token for service-account@corp expired"
        )
        with (
            patch("haute.routes.mlflow._ensure_tracking", return_value=(MagicMock(), mock_client)),
        ):
            resp = client.get("/api/mlflow/models")
        assert resp.status_code == 502
        detail = resp.json()["detail"]
        assert "service-account@corp" not in detail
        assert "Check the server logs" in detail

    def test_model_versions_502_no_leak(self, client: TestClient) -> None:
        mock_client = MagicMock()
        mock_client.search_model_versions.side_effect = RuntimeError(
            "Databricks API error: workspace /Users/admin/secret"
        )
        with (
            patch("haute.routes.mlflow._ensure_tracking", return_value=(MagicMock(), mock_client)),
            patch("haute.routes.mlflow.search_versions", side_effect=RuntimeError(
                "Databricks API error: workspace /Users/admin/secret"
            )),
        ):
            resp = client.get("/api/mlflow/model-versions", params={"model_name": "test"})
        assert resp.status_code == 502
        detail = resp.json()["detail"]
        assert "/Users/admin/secret" not in detail
        assert "Check the server logs" in detail


class TestMlflowRoutesLogOnError:
    """Verify MLflow routes log the actual error before returning safe detail."""

    def test_experiments_logs_error(self, client: TestClient) -> None:
        mock_mlflow = MagicMock()
        mock_mlflow.search_experiments.side_effect = RuntimeError("secret-mlflow-err")
        mock_logger = MagicMock()
        with (
            patch("haute.routes.mlflow._ensure_tracking", return_value=(mock_mlflow, MagicMock())),
            patch("haute.routes.mlflow.logger", mock_logger),
        ):
            client.get("/api/mlflow/experiments")
        mock_logger.error.assert_called_once()
        assert "secret-mlflow-err" in str(mock_logger.error.call_args)
