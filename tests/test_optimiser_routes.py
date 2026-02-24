"""Tests for the optimiser node type and API routes."""

from __future__ import annotations

import time

import numpy as np
import polars as pl
import pytest
from fastapi.testclient import TestClient

from haute._parser_helpers import _build_node_config, _infer_node_type
from haute._sandbox import set_project_root
from haute.graph_utils import NodeType
from haute.server import app
from tests.conftest import make_edge, make_graph


@pytest.fixture()
def client():
    return TestClient(app)


# ---------------------------------------------------------------------------
# Test data: build a scored DataFrame in the shape price-contour expects
# ---------------------------------------------------------------------------


def _make_scored_data(tmp_path, n_quotes: int = 50, n_steps: int = 5) -> str:
    """Create a scored DataFrame in long format for optimisation tests.

    Columns: quote_id, scenario_index, scenario_value, expected_income, volume
    """
    rng = np.random.RandomState(42)
    quote_ids = []
    steps = []
    mults = []
    incomes = []
    volumes = []
    scenario_values = np.linspace(0.8, 1.2, n_steps).astype(np.float32)
    for q in range(n_quotes):
        base_income = rng.uniform(100, 1000)
        base_volume = rng.uniform(0.5, 1.5)
        for s, m in enumerate(scenario_values):
            quote_ids.append(f"q_{q:04d}")
            steps.append(s)
            mults.append(float(m))
            incomes.append(float(base_income * m))
            volumes.append(float(base_volume * (2.0 - m)))
    df = pl.DataFrame({
        "quote_id": quote_ids,
        "scenario_index": pl.Series(steps, dtype=pl.Int32),
        "scenario_value": pl.Series(mults, dtype=pl.Float32),
        "expected_income": pl.Series(incomes, dtype=pl.Float32),
        "volume": pl.Series(volumes, dtype=pl.Float32),
    })
    path = tmp_path / "scored.parquet"
    df.write_parquet(path)
    return str(path)


@pytest.fixture()
def scored_data(tmp_path) -> str:
    return _make_scored_data(tmp_path)


def _make_optimiser_graph(data_path: str, config: dict | None = None) -> dict:
    """Build a 2-node graph: dataSource → optimiser."""
    default_config: dict = {
        "mode": "online",
        "objective": "expected_income",
        "constraints": {"volume": {"min": 0.90}},
        "quote_id": "quote_id",
        "scenario_index": "scenario_index",
        "scenario_value": "scenario_value",
        "max_iter": 20,
        "tolerance": 1e-4,
    }
    if config:
        default_config.update(config)

    graph = make_graph({
        "nodes": [
            {
                "id": "source",
                "data": {
                    "label": "source",
                    "nodeType": "dataSource",
                    "config": {"path": data_path},
                },
            },
            {
                "id": "opt",
                "data": {"label": "optimiser", "nodeType": "optimiser", "config": default_config},
            },
        ],
        "edges": [make_edge("source", "opt").model_dump()],
    })
    return graph.model_dump()


def _poll_until_done(client: TestClient, job_id: str, timeout: float = 30) -> dict:
    """Poll /solve/status/{job_id} until completed or error."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/api/optimiser/solve/status/{job_id}")
        assert resp.status_code == 200
        data = resp.json()
        if data["status"] in ("completed", "error"):
            return data
        time.sleep(0.1)
    raise TimeoutError(f"Job {job_id} did not finish within {timeout}s")


# ---------------------------------------------------------------------------
# Step 1: Type registration
# ---------------------------------------------------------------------------


class TestNodeTypeRegistration:
    def test_optimiser_enum_value(self):
        assert NodeType.OPTIMISER == "optimiser"

    def test_optimiser_in_nodetype(self):
        assert "optimiser" in [e.value for e in NodeType]


# ---------------------------------------------------------------------------
# Step 1c: Parser inference
# ---------------------------------------------------------------------------


class TestParserInference:
    def test_infer_optimiser_type(self):
        kwargs = {"optimiser": True, "objective": "expected_income"}
        result = _infer_node_type(kwargs, n_params=1)
        assert result == NodeType.OPTIMISER

    def test_build_optimiser_config(self):
        kwargs = {
            "optimiser": True,
            "mode": "online",
            "objective": "expected_income",
            "constraints": {"volume": {"min": 0.90}},
            "max_iter": 50,
        }
        config = _build_node_config("optimiser", kwargs, "", ["df"])
        assert config["mode"] == "online"
        assert config["objective"] == "expected_income"
        assert config["constraints"] == {"volume": {"min": 0.90}}
        assert config["max_iter"] == 50


# ---------------------------------------------------------------------------
# Step 1d: Codegen
# ---------------------------------------------------------------------------


class TestCodegen:
    def test_codegen_optimiser(self):
        from haute.codegen import _node_to_code
        from haute.graph_utils import GraphNode, NodeData

        node = GraphNode(
            id="opt",
            data=NodeData(
                label="my_optimiser",
                nodeType="optimiser",
                config={
                    "mode": "online",
                    "objective": "expected_income",
                    "constraints": {"volume": {"min": 0.90}},
                },
            ),
        )
        code = _node_to_code(node, source_names=["scored_data"])
        assert "@pipeline.node(optimiser=True" in code
        assert "def my_optimiser(" in code
        assert "scored_data: pl.LazyFrame" in code
        assert "return scored_data" in code


# ---------------------------------------------------------------------------
# Step 1e: Executor passthrough
# ---------------------------------------------------------------------------


class TestExecutorPassthrough:
    def test_optimiser_passthrough(self):
        from haute.executor import _build_node_fn
        from haute.graph_utils import GraphNode, NodeData

        node = GraphNode(
            id="opt",
            data=NodeData(
                label="optimiser",
                nodeType="optimiser",
                config={"mode": "online"},
            ),
        )
        func_name, fn, is_source = _build_node_fn(node, source_names=["df"])
        assert func_name == "optimiser"
        assert is_source is False

        # Should pass through the input unchanged
        input_df = pl.LazyFrame({"a": [1, 2, 3]})
        result = fn(input_df)
        assert result.collect().to_dicts() == input_df.collect().to_dicts()


# ---------------------------------------------------------------------------
# Step 2: API routes
# ---------------------------------------------------------------------------


class TestSolveRoute:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_solve_returns_started(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"
        assert data["job_id"]

    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_solve_completes(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        data = resp.json()
        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed"
        result = status["result"]
        assert "total_objective" in result
        assert "lambdas" in result
        assert "converged" in result

    def test_solve_missing_node(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "nonexistent"})
        assert resp.status_code == 404

    def test_solve_wrong_node_type(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "source"})
        assert resp.status_code == 400

    def test_solve_no_objective(self, client, scored_data):
        cfg = {"objective": "", "constraints": {"v": {"min": 0.9}}}
        graph = _make_optimiser_graph(scored_data, config=cfg)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        assert resp.status_code == 400
        assert "objective" in resp.json()["detail"].lower()

    def test_solve_no_constraints(self, client, scored_data):
        cfg = {"objective": "expected_income", "constraints": {}}
        graph = _make_optimiser_graph(scored_data, config=cfg)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        assert resp.status_code == 400
        assert "constraint" in resp.json()["detail"].lower()


class TestStatusRoute:
    def test_missing_job_returns_404(self, client):
        resp = client.get("/api/optimiser/solve/status/nonexistent")
        assert resp.status_code == 404


class TestApplyRoute:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_apply_after_solve(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        job_id = resp.json()["job_id"]
        _poll_until_done(client, job_id)

        resp = client.post("/api/optimiser/apply", json={"job_id": job_id})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["row_count"] > 0
        assert "total_objective" in data

    def test_apply_missing_job(self, client):
        resp = client.post("/api/optimiser/apply", json={"job_id": "nonexistent"})
        assert resp.status_code == 404


class TestSaveRoute:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_save_after_solve(self, client, scored_data, tmp_path):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post("/api/optimiser/solve", json={"graph": graph, "node_id": "opt"})
        job_id = resp.json()["job_id"]
        _poll_until_done(client, job_id)

        out_path = str(tmp_path / "result.json")
        resp = client.post("/api/optimiser/save", json={"job_id": job_id, "output_path": out_path})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["path"] == out_path

        import json
        saved = json.loads((tmp_path / "result.json").read_text())
        assert "lambdas" in saved

    def test_save_missing_job(self, client):
        resp = client.post(
            "/api/optimiser/save",
            json={"job_id": "nonexistent", "output_path": "/tmp/x.json"},
        )
        assert resp.status_code == 404

    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_save_path_traversal_blocked(self, client, scored_data, tmp_path):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        _poll_until_done(client, job_id)

        # Narrow the sandbox so the traversal path escapes it
        set_project_root(tmp_path)
        resp = client.post(
            "/api/optimiser/save",
            json={"job_id": job_id, "output_path": "../../etc/passwd"},
        )
        assert resp.status_code == 403
        assert "outside" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Ratebook mode
# ---------------------------------------------------------------------------


def _make_ratebook_data(tmp_path, n_quotes: int = 50, n_steps: int = 5):
    """Create scored + banding DataFrames for ratebook tests.

    Returns (scored_path, banding_path).
    """
    rng = np.random.RandomState(42)
    regions = ["North", "South", "East"]

    # Build scored data in long format
    quote_ids = []
    steps = []
    mults = []
    incomes = []
    volumes = []
    scenario_values = np.linspace(0.8, 1.2, n_steps).astype(np.float32)
    for q in range(n_quotes):
        base_income = rng.uniform(100, 1000)
        base_volume = rng.uniform(0.5, 1.5)
        for s, m in enumerate(scenario_values):
            quote_ids.append(f"q_{q:04d}")
            steps.append(s)
            mults.append(float(m))
            incomes.append(float(base_income * m))
            volumes.append(float(base_volume * (2.0 - m)))

    scored_df = pl.DataFrame({
        "quote_id": quote_ids,
        "scenario_index": pl.Series(steps, dtype=pl.Int32),
        "scenario_value": pl.Series(mults, dtype=pl.Float32),
        "expected_income": pl.Series(incomes, dtype=pl.Float32),
        "volume": pl.Series(volumes, dtype=pl.Float32),
    })
    scored_path = tmp_path / "scored.parquet"
    scored_df.write_parquet(scored_path)

    # Build banding data: one row per quote with a region factor
    banding_df = pl.DataFrame({
        "quote_id": [f"q_{q:04d}" for q in range(n_quotes)],
        "region": [regions[q % len(regions)] for q in range(n_quotes)],
    })
    banding_path = tmp_path / "banding.parquet"
    banding_df.write_parquet(banding_path)

    return str(scored_path), str(banding_path)


def _make_ratebook_graph(data_path: str, banding_data_path: str) -> dict:
    """Build a 3-node graph: dataSource → optimiser ← banding."""
    graph = make_graph({
        "nodes": [
            {
                "id": "source",
                "data": {
                    "label": "source",
                    "nodeType": "dataSource",
                    "config": {"path": data_path},
                },
            },
            {
                "id": "banding",
                "data": {
                    "label": "banding",
                    "nodeType": "dataSource",
                    "config": {"path": banding_data_path},
                },
            },
            {
                "id": "opt",
                "data": {
                    "label": "optimiser",
                    "nodeType": "optimiser",
                    "config": {
                        "mode": "ratebook",
                        "objective": "expected_income",
                        "constraints": {"volume": {"min": 0.90}},
                        "quote_id": "quote_id",
                        "scenario_index": "scenario_index",
                        "scenario_value": "scenario_value",
                        "max_iter": 20,
                        "tolerance": 1e-4,
                        "max_cd_iterations": 5,
                        "cd_tolerance": 1e-3,
                        "factor_columns": [["region"]],
                        "banding_source": "banding",
                        "data_input": "source",
                    },
                },
            },
        ],
        "edges": [
            make_edge("source", "opt").model_dump(),
            make_edge("banding", "opt").model_dump(),
        ],
    })
    return graph.model_dump()


class TestRatebookSolve:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_ratebook_solve_completes(self, client, tmp_path):
        scored_path, banding_path = _make_ratebook_data(tmp_path)
        graph = _make_ratebook_graph(scored_path, banding_path)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"

        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed", status.get("message", "")
        result = status["result"]
        assert result["mode"] == "ratebook"
        assert "factor_tables" in result
        assert "converged" in result
        assert "lambdas" in result

    def test_ratebook_no_factor_columns(self, client, scored_data):
        graph = _make_optimiser_graph(
            scored_data,
            config={
                "mode": "ratebook",
                "objective": "expected_income",
                "constraints": {"volume": {"min": 0.9}},
                "factor_columns": [],
            },
        )
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        assert resp.status_code == 400
        assert "factor_columns" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Phase 1 tests
# ---------------------------------------------------------------------------


class TestSolveWithHistory:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_solve_with_history(self, client, scored_data):
        graph = _make_optimiser_graph(
            scored_data, config={"record_history": True}
        )
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        status = _poll_until_done(client, job_id)
        assert status["status"] == "completed"
        result = status["result"]
        assert "history" in result
        history = result["history"]
        assert isinstance(history, list)
        assert len(history) > 0
        first = history[0]
        assert "iteration" in first
        assert "total_objective" in first


class TestScenarioValueStats:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_scenario_value_stats_in_result(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        status = _poll_until_done(client, job_id)
        assert status["status"] == "completed"
        result = status["result"]
        assert "scenario_value_stats" in result
        stats = result["scenario_value_stats"]
        assert "mean" in stats
        assert "p50" in stats
        assert "pct_increase" in stats
        assert "scenario_value_histogram" in result
        hist = result["scenario_value_histogram"]
        assert "counts" in hist
        assert "edges" in hist
        assert len(hist["counts"]) == 20
        assert len(hist["edges"]) == 21


class TestColumnValidation:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_solve_missing_column(self, client, tmp_path):
        """Data without a constraint column returns 400."""
        df = pl.DataFrame({
            "quote_id": ["q_0"] * 5,
            "scenario_index": pl.Series(range(5), dtype=pl.Int32),
            "scenario_value": pl.Series(
                np.linspace(0.8, 1.2, 5).tolist(), dtype=pl.Float32
            ),
            "expected_income": pl.Series(
                [100.0] * 5, dtype=pl.Float32
            ),
            # no "volume" column!
        })
        path = str(tmp_path / "no_volume.parquet")
        df.write_parquet(path)
        graph = _make_optimiser_graph(path)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        assert resp.status_code == 400
        assert "volume" in resp.json()["detail"]


class TestNonConvergenceWarning:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_solve_non_convergence_warning(self, client, scored_data):
        graph = _make_optimiser_graph(
            scored_data, config={"max_iter": 1}
        )
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        status = _poll_until_done(client, job_id)
        assert status["status"] == "completed"
        result = status["result"]
        if not result["converged"]:
            assert "warning" in result


class TestSaveEndpointFields:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_save_has_full_fields(self, client, scored_data, tmp_path):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        _poll_until_done(client, job_id)

        out_path = str(tmp_path / "result.json")
        resp = client.post(
            "/api/optimiser/save",
            json={"job_id": job_id, "output_path": out_path},
        )
        assert resp.status_code == 200

        import json as json_mod
        saved = json_mod.loads((tmp_path / "result.json").read_text())
        assert "lambdas" in saved
        assert "mode" in saved
        assert saved["mode"] == "online"
        assert "baseline_objective" in saved
        assert "baseline_constraints" in saved
        assert "constraints" in saved
        assert "objective" in saved
        assert "quote_id" in saved
        assert "chunk_size" in saved


# ---------------------------------------------------------------------------
# Phase 2a: Frontier
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Phase 4: Step-wise expansion
# ---------------------------------------------------------------------------


def _make_base_data(tmp_path, n_quotes: int = 50) -> str:
    """Create base data (one row per quote, no scenario expansion)."""
    rng = np.random.RandomState(42)
    df = pl.DataFrame({
        "quote_id": [f"q_{q:04d}" for q in range(n_quotes)],
        "base_income": pl.Series(
            rng.uniform(100, 1000, n_quotes).tolist(),
            dtype=pl.Float64,
        ),
        "base_volume": pl.Series(
            rng.uniform(0.5, 1.5, n_quotes).tolist(),
            dtype=pl.Float64,
        ),
    })
    path = tmp_path / "base.parquet"
    df.write_parquet(path)
    return str(path)


def _make_expander_graph(data_path: str) -> dict:
    """Build a 4-node graph: dataSource → expander → transform → optimiser.

    The expander cross-joins scenario_value and scenario_index columns.
    The transform computes objective and constraint columns.
    """
    graph = make_graph({
        "nodes": [
            {
                "id": "source",
                "data": {
                    "label": "source",
                    "nodeType": "dataSource",
                    "config": {"path": data_path},
                },
            },
            {
                "id": "transform",
                "data": {
                    "label": "compute_metrics",
                    "nodeType": "transform",
                    "config": {
                        "code": (
                            "df = df.with_columns([\n"
                            "    (pl.col('base_income') * "
                            "pl.col('scenario_value'))"
                            ".alias('expected_income'),\n"
                            "    (pl.col('base_volume') * "
                            "(2.0 - pl.col('scenario_value')))"
                            ".alias('volume'),\n"
                            "])"
                        ),
                    },
                },
            },
            {
                "id": "expander",
                "data": {
                    "label": "expander",
                    "nodeType": "scenarioExpander",
                    "config": {
                        "column_name": "scenario_value",
                        "min_value": 0.8,
                        "max_value": 1.2,
                        "steps": 5,
                        "step_column": "scenario_index",
                    },
                },
            },
            {
                "id": "opt",
                "data": {
                    "label": "optimiser",
                    "nodeType": "optimiser",
                    "config": {
                        "mode": "online",
                        "objective": "expected_income",
                        "constraints": {"volume": {"min": 0.90}},
                        "quote_id": "quote_id",
                        "scenario_index": "scenario_index",
                        "scenario_value": "scenario_value",
                        "max_iter": 20,
                        "tolerance": 1e-4,
                    },
                },
            },
        ],
        "edges": [
            make_edge("source", "expander").model_dump(),
            make_edge("expander", "transform").model_dump(),
            make_edge("transform", "opt").model_dump(),
        ],
    })
    return graph.model_dump()


class TestExpanderSolve:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_expander_solve_completes(self, client, tmp_path):
        data_path = _make_base_data(tmp_path)
        graph = _make_expander_graph(data_path)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"

        status = _poll_until_done(client, data["job_id"])
        assert status["status"] == "completed", status.get("message", "")
        result = status["result"]
        assert "total_objective" in result
        assert "lambdas" in result
        assert result["n_steps"] == 5

    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_normal_solve_produces_lambdas(self, client, tmp_path):
        """Normal solve should complete and produce lambdas."""
        scored_path = _make_scored_data(tmp_path, n_quotes=50, n_steps=5)
        normal_graph = _make_optimiser_graph(scored_path)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": normal_graph, "node_id": "opt"},
        )
        normal_status = _poll_until_done(client, resp.json()["job_id"])
        assert normal_status["status"] == "completed"
        normal_result = normal_status["result"]
        assert len(normal_result["lambdas"]) > 0
        assert "total_objective" in normal_result


class TestFrontierRoute:
    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_frontier_after_solve(self, client, scored_data):
        graph = _make_optimiser_graph(scored_data)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        _poll_until_done(client, job_id)

        resp = client.post(
            "/api/optimiser/frontier",
            json={
                "job_id": job_id,
                "threshold_ranges": {"volume": [0.85, 0.95]},
                "n_points_per_dim": 3,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["n_points"] > 0
        assert len(data["points"]) == data["n_points"]
        assert "volume" in data["constraint_names"]

    def test_frontier_missing_job(self, client):
        resp = client.post(
            "/api/optimiser/frontier",
            json={
                "job_id": "nonexistent",
                "threshold_ranges": {"volume": [0.85, 0.95]},
            },
        )
        assert resp.status_code == 404

    @pytest.mark.usefixtures("_widen_sandbox_root")
    def test_frontier_incomplete_job(self, client, scored_data):
        """Frontier on a not-yet-completed job returns 400."""
        graph = _make_optimiser_graph(scored_data)
        resp = client.post(
            "/api/optimiser/solve",
            json={"graph": graph, "node_id": "opt"},
        )
        job_id = resp.json()["job_id"]
        # Don't poll — submit frontier immediately
        # Job may already be complete for small data; handle both
        resp = client.post(
            "/api/optimiser/frontier",
            json={
                "job_id": job_id,
                "threshold_ranges": {"volume": [0.85, 0.95]},
            },
        )
        # Either 400 (still running) or 200 (already done) is acceptable
        assert resp.status_code in (200, 400)
