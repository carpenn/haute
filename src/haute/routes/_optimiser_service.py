"""OptimiserSolveService — orchestrates optimisation solving, extracted from the route handler.

The route handler becomes a thin adapter that delegates to
``OptimiserSolveService.start()``.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Any

import numpy as np
from fastapi import HTTPException

if TYPE_CHECKING:
    import polars as pl
    from price_contour import QuoteGrid

from haute._logging import get_logger
from haute._types import (
    GraphNode,
    OnlineSolveResultLike,
    PipelineGraph,
    RatebookSolveResultLike,
    SolveResultLike,
)
from haute.graph_utils import NodeType
from haute.routes._helpers import raise_node_not_found, raise_node_type_error
from haute.routes._job_store import JobStore
from haute.schemas import OptimiserSolveRequest, OptimiserSolveResponse

logger = get_logger(component="server.optimiser")

# ── Default constants ─────────────────────────────────────────────
_DEFAULT_TIMEOUT = 300  # seconds — max wall-clock time for a solve job
_HISTOGRAM_BINS = 20  # bin count for scenario-value distribution histogram
_DEFAULT_MAX_ITER = 50  # max solver iterations (online & ratebook)
_DEFAULT_CHUNK_SIZE = 500_000  # rows per chunk for solver processing
_DEFAULT_TOLERANCE = 1e-6  # convergence tolerance for solver
_DEFAULT_MAX_CD_ITERATIONS = 10  # max coordinate-descent iterations (ratebook)
_DEFAULT_CD_TOLERANCE = 1e-3  # coordinate-descent convergence tolerance (ratebook)


def _find_optimiser_node(graph: PipelineGraph, node_id: str) -> GraphNode:
    """Find and validate an optimiser node in the graph."""
    node = graph.node_map.get(node_id)
    if node is None:
        raise_node_not_found(node_id)
    if node.data.nodeType != NodeType.OPTIMISER:
        raise_node_type_error(node_id, "optimiser", str(node.data.nodeType))
    return node


def _compute_scenario_value_stats(
    solve_result: SolveResultLike,
) -> tuple[dict[str, float], dict[str, list[float]]]:
    """Compute scenario value distribution statistics and histogram from solve result."""
    if not hasattr(solve_result, "dataframe"):
        return {}, {}
    df = solve_result.dataframe
    if "optimal_scenario_value" not in df.columns:
        return {}, {}

    col = df["optimal_scenario_value"]
    n = len(col)
    stats = {
        "mean": float(col.mean()),
        "std": float(col.std()),
        "min": float(col.min()),
        "max": float(col.max()),
        "p5": float(col.quantile(0.05)),
        "p25": float(col.quantile(0.25)),
        "p50": float(col.quantile(0.50)),
        "p75": float(col.quantile(0.75)),
        "p95": float(col.quantile(0.95)),
        "pct_increase": float((col > 1.0).sum() / n) if n else 0.0,
        "pct_decrease": float((col < 1.0).sum() / n) if n else 0.0,
    }

    vals = col.to_numpy()
    counts, edges = np.histogram(vals, bins=_HISTOGRAM_BINS)
    histogram = {
        "counts": [int(c) for c in counts],
        "edges": [float(e) for e in edges],
    }
    return stats, histogram


def _solve_online(
    quote_grid: QuoteGrid,
    config: dict[str, Any],
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the online optimiser solver on a pre-built QuoteGrid."""
    from price_contour import OnlineOptimiser

    solver = OnlineOptimiser(
        objective=config["objective"],
        constraints=config["constraints"],
        max_iter=config.get("max_iter", _DEFAULT_MAX_ITER),
        chunk_size=config.get("chunk_size", _DEFAULT_CHUNK_SIZE),
        tolerance=config.get("tolerance", _DEFAULT_TOLERANCE),
        record_history=config.get("record_history", False),
    )
    solve_result: OnlineSolveResultLike = solver.solve(quote_grid)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="online", elapsed=f"{elapsed:.2f}s", converged=converged)

    scenario_value_stats, scenario_value_histogram = _compute_scenario_value_stats(solve_result)

    result_dict: dict[str, Any] = {
        "mode": "online",
        "total_objective": solve_result.total_objective,
        "baseline_objective": solve_result.baseline_objective,
        "constraints": solve_result.total_constraints,
        "baseline_constraints": solve_result.baseline_constraints,
        "lambdas": solve_result.lambdas,
        "converged": solve_result.converged,
        "iterations": solve_result.iterations,
        "n_quotes": solve_result.n_quotes,
        "n_steps": solve_result.n_steps,
        "history": solve_result.history if config.get("record_history") else None,
        "scenario_value_stats": scenario_value_stats,
        "scenario_value_histogram": scenario_value_histogram,
    }
    if not solve_result.converged:
        result_dict["warning"] = (
            "Solver did not converge. Consider increasing max_iter or relaxing tolerance."
        )

    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "quote_grid": solve_result.grid,
        "result": result_dict,
    })


def _solve_ratebook(
    quote_grid: QuoteGrid,
    config: dict[str, Any],
    factors_df: pl.DataFrame | None,
    job: dict[str, Any],
    start_time: float,
) -> None:
    """Run the ratebook optimiser solver on a pre-built QuoteGrid."""
    import polars as pl
    from price_contour import RatebookOptimiser

    if factors_df is None:
        raise RuntimeError(
            "Ratebook mode requires a banding source. "
            "Select a banding node in the Rating Factor Source dropdown."
        )

    constraints = config["constraints"]
    qid_col = config.get("quote_id", "quote_id")

    raw_factor_columns = config.get("factor_columns", [])
    available_cols = set(factors_df.columns)
    factor_columns_valid = [
        group for group in raw_factor_columns
        if all(c in available_cols for c in group)
    ]
    if not factor_columns_valid:
        missing = [c for group in raw_factor_columns for c in group if c not in available_cols]
        raise RuntimeError(
            f"No valid factor groups found. Missing columns in banding source: {missing}. "
            f"Available columns: {sorted(available_cols)}"
        )

    solver = RatebookOptimiser(
        objective=config["objective"],
        constraints=constraints,
        factor_columns=factor_columns_valid,
        max_iter=config.get("max_iter", _DEFAULT_MAX_ITER),
        max_cd_iterations=config.get("max_cd_iterations", _DEFAULT_MAX_CD_ITERATIONS),
        cd_tolerance=config.get("cd_tolerance", _DEFAULT_CD_TOLERANCE),
        tolerance=config.get("tolerance", _DEFAULT_TOLERANCE),
        chunk_size=config.get("chunk_size", _DEFAULT_CHUNK_SIZE),
    )

    factor_cols_flat = [c for group in factor_columns_valid for c in group]
    avail = factors_df.columns
    if qid_col in avail:
        keep = [qid_col] + [c for c in factor_cols_flat if c in avail]
        factors_df = factors_df.select(keep)
        if qid_col != "quote_id":
            factors_df = factors_df.rename({qid_col: "quote_id"})
    elif "quote_id" in avail:
        keep = ["quote_id"] + [c for c in factor_cols_flat if c in avail]
        factors_df = factors_df.select(keep)

    factors_df = factors_df.with_columns(pl.col("quote_id").cast(pl.Utf8))
    factors_df = factors_df.unique(subset=["quote_id"])

    quote_order = pl.DataFrame({"quote_id": quote_grid.quote_ids})
    quote_order = quote_order.unique(maintain_order=True)
    factors_df = quote_order.join(factors_df, on="quote_id", how="left")
    factors_df = factors_df.drop("quote_id")

    solve_result: RatebookSolveResultLike = solver.solve(quote_grid, factors_df)
    elapsed = time.monotonic() - start_time
    converged = solve_result.converged
    logger.info("solve_completed", mode="ratebook", elapsed=f"{elapsed:.2f}s", converged=converged)

    factor_tables_serialised = {}
    for name, table in solve_result.factor_tables.items():
        factor_tables_serialised[name] = [
            {"__factor_group__": level, "optimal_scenario_value": sv}
            for level, sv in table.items()
        ]

    scenario_value_stats, scenario_value_histogram = _compute_scenario_value_stats(solve_result)

    result_dict: dict[str, Any] = {
        "mode": "ratebook",
        "total_objective": solve_result.total_objective,
        "baseline_objective": solve_result.baseline_objective,
        "constraints": solve_result.total_constraints,
        "baseline_constraints": solve_result.baseline_constraints,
        "lambdas": solve_result.lambdas,
        "converged": solve_result.converged,
        "cd_iterations": solve_result.cd_iterations,
        "factor_tables": factor_tables_serialised,
        "clamp_rate": getattr(solve_result, "clamp_rate", None),
        "history": None,
        "scenario_value_stats": scenario_value_stats,
        "scenario_value_histogram": scenario_value_histogram,
    }
    if not solve_result.converged:
        result_dict["warning"] = (
            "Solver did not converge. Consider increasing max_iter or relaxing tolerance."
        )

    job.update({
        "status": "completed",
        "progress": 1.0,
        "message": "Completed",
        "elapsed_seconds": elapsed,
        "solver": solver,
        "solve_result": solve_result,
        "quote_grid": quote_grid,
        "result": result_dict,
    })


class OptimiserSolveService:
    """Orchestrates the full optimisation solve lifecycle.

    Parameters
    ----------
    store:
        The in-memory job store used to track optimisation jobs.
    """

    def __init__(self, store: JobStore) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def start(self, body: OptimiserSolveRequest) -> OptimiserSolveResponse:
        """Validate config, execute pipeline, build grid, and launch solver.

        Returns an ``OptimiserSolveResponse`` with status ``"started"``.
        Raises ``HTTPException`` on validation or pipeline failures.
        """
        node = _find_optimiser_node(body.graph, body.node_id)
        config = node.data.config

        mode = self._validate_config(config)

        job_id = self._store.create_job({
            "status": "running",
            "progress": 0.0,
            "message": "Starting",
            "config": dict(config),
            "node_label": node.data.label,
        })
        logger.info("solve_started", node_id=body.node_id, mode=mode, job_id=job_id)

        lazy_outputs = self._execute_pipeline(body, job_id)
        source_lf = self._resolve_data_source(lazy_outputs, config, body.node_id, job_id)
        constraint_cols, scored_lf = self._validate_and_project(
            source_lf, config, job_id,
        )
        factors_df = self._extract_factors(lazy_outputs, config, mode)
        del lazy_outputs

        quote_grid = self._build_grid(
            scored_lf, constraint_cols, config, body.node_id, job_id,
        )
        self._launch_background(job_id, body.node_id, config, mode, quote_grid, factors_df)
        return OptimiserSolveResponse(status="started", job_id=job_id)

    # ------------------------------------------------------------------
    # Private orchestration steps
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_config(config: dict[str, Any]) -> str:
        """Validate optimiser config; return the mode ('online' or 'ratebook')."""
        objective = config.get("objective")
        if not objective:
            raise HTTPException(
                status_code=400,
                detail="No objective column configured."
                " Open the config panel and set an objective.",
            )

        constraints = config.get("constraints")
        if not constraints:
            raise HTTPException(
                status_code=400,
                detail="No constraints configured. Add at least one constraint.",
            )

        mode = config.get("mode", "online")
        if mode not in ("online", "ratebook"):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported optimiser mode '{mode}'."
                " Currently supported: online, ratebook.",
            )

        if mode == "ratebook":
            factor_columns = config.get("factor_columns")
            if not factor_columns:
                raise HTTPException(
                    status_code=400,
                    detail="Ratebook mode requires factor_columns. Add at least one factor group.",
                )

        return mode

    def _execute_pipeline(
        self, body: OptimiserSolveRequest, job_id: str,
    ) -> dict[str, Any]:
        """Execute the pipeline lazily up to the optimiser node."""
        try:
            from haute.executor import _build_node_fn
            from haute.graph_utils import _execute_lazy

            lazy_outputs, *_ = _execute_lazy(
                body.graph, _build_node_fn,
                target_node_id=body.node_id,
            )
            return lazy_outputs
        except Exception as exc:
            error_msg = f"Pipeline execution failed: {exc}"
            logger.error("pipeline_exec_failed", error=str(exc), node_id=body.node_id)
            self._store.jobs[job_id] = {"status": "error", "message": error_msg}
            raise HTTPException(status_code=500, detail=error_msg)

    def _resolve_data_source(
        self,
        lazy_outputs: dict[str, Any],
        config: dict[str, Any],
        node_id: str,
        job_id: str,
    ) -> Any:
        """Pick the correct lazy source from pipeline outputs."""
        data_input_id = config.get("data_input")
        if data_input_id and data_input_id in lazy_outputs:
            source_lf = lazy_outputs[data_input_id]
        else:
            source_lf = lazy_outputs.get(node_id)

        if source_lf is None:
            error_msg = (
                "No data arrived at the optimiser node. "
                "Make sure an upstream data source is connected and producing data."
            )
            self._store.jobs[job_id] = {"status": "error", "message": error_msg}
            raise HTTPException(status_code=400, detail=error_msg)

        return source_lf

    def _validate_and_project(
        self,
        source_lf: Any,
        config: dict[str, Any],
        job_id: str,
    ) -> tuple[list[str], Any]:
        """Validate columns and build the projection for the solver.

        Returns (constraint_cols, projected_lazy_frame).
        """
        import polars as pl

        objective = config["objective"]
        constraints = config["constraints"]
        qid_col = config.get("quote_id", "quote_id")
        mult_col = config.get("scenario_value", "scenario_value")
        step_col = config.get("scenario_index", "scenario_index")

        available_cols = set(source_lf.collect_schema().names())
        required_cols = {objective, qid_col, mult_col, step_col}
        for cname in constraints:
            required_cols.add(cname)
        missing_cols = sorted(required_cols - available_cols)
        if missing_cols:
            avail = sorted(available_cols)
            detail = f"Missing columns in scored data: {missing_cols}. Available: {avail}"
            self._store.jobs[job_id] = {"status": "error", "message": detail}
            raise HTTPException(status_code=400, detail=detail)

        constraint_cols = list(constraints.keys()) if isinstance(constraints, dict) else []

        solver_cols = [qid_col, step_col, mult_col, objective] + [
            c for c in constraint_cols if c in available_cols
        ]
        cast_map: dict[str, pl.DataType] = {
            qid_col: pl.Utf8,
            step_col: pl.Int32,
            mult_col: pl.Float32,
            objective: pl.Float32,
        }
        for c in constraint_cols:
            cast_map[c] = pl.Float32
        cast_exprs = [pl.col(c).cast(t) for c, t in cast_map.items()]

        scored_lf = (
            source_lf
            .select(solver_cols)
            .with_columns(cast_exprs)
            .filter(pl.col(qid_col).is_not_null())
        )
        return constraint_cols, scored_lf

    @staticmethod
    def _extract_factors(
        lazy_outputs: dict[str, Any],
        config: dict[str, Any],
        mode: str,
    ) -> Any:
        """Extract ratebook factors DataFrame (None for online mode)."""
        if mode != "ratebook":
            return None
        banding_source_id = config.get("banding_source")
        if banding_source_id and banding_source_id in lazy_outputs:
            return lazy_outputs[banding_source_id].collect(engine="streaming")
        return None

    def _build_grid(
        self,
        scored_lf: Any,
        constraint_cols: list[str],
        config: dict[str, Any],
        node_id: str,
        job_id: str,
    ) -> QuoteGrid:
        """Sink scored data to parquet and build the QuoteGrid."""
        from price_contour import build_grid_from_parquet

        objective = config["objective"]
        qid_col = config.get("quote_id", "quote_id")
        mult_col = config.get("scenario_value", "scenario_value")
        step_col = config.get("scenario_index", "scenario_index")

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(tmp_fd)
        try:
            scored_lf.sink_parquet(tmp_path)
            del scored_lf

            quote_grid = build_grid_from_parquet(
                tmp_path,
                constraint_cols,
                quote_id=qid_col,
                scenario_index=step_col,
                scenario_value_col=mult_col,
                objective=objective,
            )
        except HTTPException:
            raise
        except Exception as exc:
            detail = f"Grid construction failed: {exc}"
            logger.error("grid_build_failed", error=str(exc), node_id=node_id)
            self._store.update_job(job_id, status="error", message=detail)
            raise HTTPException(status_code=400, detail=detail) from exc
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        return quote_grid

    def _launch_background(
        self,
        job_id: str,
        node_id: str,
        config: dict[str, Any],
        mode: str,
        quote_grid: QuoteGrid,
        factors_df: Any,
    ) -> None:
        """Start the solver in a background thread."""
        start_time = time.monotonic()
        self._store.update_job(job_id,
            start_time=start_time,
            timeout=config.get("timeout", _DEFAULT_TIMEOUT),
        )

        def _solve_background() -> None:
            try:
                job = self._store.jobs[job_id]
                job.update({
                    "message": "Solving",
                    "progress": 0.1,
                    "elapsed_seconds": time.monotonic() - start_time,
                })
                if mode == "ratebook":
                    _solve_ratebook(
                        quote_grid, config, factors_df, job, start_time,
                    )
                else:
                    _solve_online(quote_grid, config, job, start_time)
            except ValueError as exc:
                error_msg = f"Data error: {exc}"
                logger.error("solve_failed", error=str(exc), node_id=node_id, category="data")
                self._store.update_job(job_id,
                    status="error",
                    message=error_msg,
                    elapsed_seconds=time.monotonic() - start_time,
                )
            except RuntimeError as exc:
                error_msg = f"Algorithm error: {exc}"
                logger.error("solve_failed", error=str(exc), node_id=node_id, category="algorithm")
                self._store.update_job(job_id,
                    status="error",
                    message=error_msg,
                    elapsed_seconds=time.monotonic() - start_time,
                )
            except Exception as exc:
                error_msg = f"Unexpected error: {exc}"
                logger.error("solve_failed", error=str(exc), node_id=node_id, category="unexpected")
                self._store.update_job(job_id,
                    status="error",
                    message=error_msg,
                    elapsed_seconds=time.monotonic() - start_time,
                )

        thread = threading.Thread(target=_solve_background, daemon=True)
        thread.start()
