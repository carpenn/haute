"""Encapsulates MODEL_SCORE node logic: model loading, prediction, post-processing.

Extracted from executor.py to reduce the size and nesting of ``_build_node_fn``
while keeping behaviour identical.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from haute._logging import get_logger
from haute._types import _Frame

logger = get_logger(component="model_scorer")

_SCORE_BATCH_SIZE = 500_000


class ModelScorer:
    """Load an MLflow model and score a LazyFrame.

    Encapsulates the full MODEL_SCORE lifecycle:
    1. Model loading (from MLflow run or registered model).
    2. Feature intersection (skip features absent from input).
    3. Prediction (eager in-memory or batched via parquet).
    4. Optional post-processing user code.

    Parameters
    ----------
    source_type : str
        ``"run"`` or ``"registered"`` — how to locate the model in MLflow.
    run_id : str
        MLflow run ID (used when *source_type* is ``"run"``).
    artifact_path : str
        Artifact path within the run (e.g. ``"model.cbm"``).
    registered_model : str
        Registered model name (used when *source_type* is ``"registered"``).
    version : str
        Model version string (``"1"``, ``"2"``, or ``"latest"``).
    task : str
        ``"regression"`` or ``"classification"``.
    output_col : str
        Name of the column that receives predictions.
    code : str
        Optional user post-processing code applied after scoring.
    source_names : list[str]
        Sanitised upstream node names (variable names for user code).
    scenario : str
        Active execution scenario — ``"live"`` uses eager scoring, anything
        else uses the batched parquet path.
    row_limit : int | None
        When set (preview/trace), forces the eager path regardless of scenario.
    """

    def __init__(
        self,
        *,
        source_type: str,
        run_id: str = "",
        artifact_path: str = "",
        registered_model: str = "",
        version: str = "latest",
        task: str = "regression",
        output_col: str = "prediction",
        code: str = "",
        source_names: list[str] | None = None,
        scenario: str = "live",
        row_limit: int | None = None,
    ) -> None:
        self.source_type = source_type
        self.run_id = run_id
        self.artifact_path = artifact_path
        self.registered_model = registered_model
        self.version = version
        self.task = task
        self.output_col = output_col
        self.code = code
        self.source_names = list(source_names) if source_names else []
        self.scenario = scenario
        self.row_limit = row_limit

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def score(self, *dfs: _Frame) -> _Frame:
        """Load the model, predict, and optionally post-process.

        Accepts one or more upstream LazyFrames (first is the scoring input).
        Returns a LazyFrame with prediction column(s) appended.
        """
        from haute._mlflow_io import load_mlflow_model

        model = load_mlflow_model(
            source_type=self.source_type,
            run_id=self.run_id,
            artifact_path=self.artifact_path,
            registered_model=self.registered_model,
            version=self.version,
            task=self.task,
        )

        lf = dfs[0] if dfs else pl.LazyFrame()
        available_cols = set(lf.collect_schema().names())
        features = [f for f in model.feature_names_ if f in available_cols]

        # Eager path: "live" scenario or row-limited preview/trace
        # (data is small, no need for disk round-trip).
        # Batched path: non-live production scoring (large data, low memory).
        if self.scenario == "live" or self.row_limit:
            result_lf = self._score_eager(model, lf, features)
        else:
            result_lf = self._score_batched(model, lf, features)

        if self.code:
            from haute.executor import _exec_user_code

            result_lf = _exec_user_code(
                self.code, self.source_names, (result_lf,),
                extra_ns={"model": model},
            )
        return result_lf

    # ------------------------------------------------------------------
    # Scoring strategies
    # ------------------------------------------------------------------

    def _score_eager(
        self, model: Any, lf: pl.LazyFrame, features: list[str],
    ) -> pl.LazyFrame:
        """Collect and score in-memory -- delegates to shared helper."""
        from haute._mlflow_io import _score_eager as score_eager_

        return score_eager_(model, lf, features, self.output_col, self.task)

    def _score_batched(
        self, model: Any, lf: pl.LazyFrame, features: list[str],
    ) -> pl.LazyFrame:
        """Sink -> batch score -> lazy scan -- low-memory path."""
        import atexit
        import os

        input_path = _sink_to_temp(lf)
        scored_path = _batch_score_to_parquet(
            model, input_path, features,
            self.output_col, self.task,
        )
        atexit.register(
            lambda p=scored_path: os.unlink(p)
            if os.path.exists(p) else None,
        )
        os.unlink(input_path)
        return pl.scan_parquet(scored_path)


# ----------------------------------------------------------------------
# Module-level helpers (shared by the class, kept out of the class body
# because they are pure functions with no dependency on instance state).
# ----------------------------------------------------------------------


def _sink_to_temp(lf: pl.LazyFrame) -> str:
    """Sink a LazyFrame to a temp parquet file via streaming."""
    import os
    import tempfile

    fd, path = tempfile.mkstemp(
        suffix=".parquet", prefix="haute_score_in_",
    )
    os.close(fd)
    try:
        lf.sink_parquet(path)
    except Exception:
        lf.collect(engine="streaming").write_parquet(path)
    return path


def _batch_score_to_parquet(
    model: Any,
    input_path: str,
    features: list[str],
    output_col: str,
    task: str,
) -> str:
    """Score a parquet file in batches, return path to scored output."""
    import os
    import tempfile

    import pyarrow.parquet as pq

    from haute._mlflow_io import _prepare_predict_frame

    fd, out_path = tempfile.mkstemp(
        suffix=".parquet", prefix="haute_score_out_",
    )
    os.close(fd)

    pf = pq.ParquetFile(input_path)
    writer = None
    want_proba = (
        task == "classification"
        and hasattr(model, "predict_proba")
    )

    try:
        for batch in pf.iter_batches(
            batch_size=_SCORE_BATCH_SIZE,
        ):
            chunk = pl.from_arrow(batch)
            x_data = _prepare_predict_frame(
                model, chunk, features,
            )
            preds = model.predict(x_data).flatten()
            chunk = chunk.with_columns(
                pl.Series(output_col, preds),
            )
            if want_proba:
                probas = model.predict_proba(x_data)
                if probas.ndim == 2:
                    probas = probas[:, 1]
                chunk = chunk.with_columns(
                    pl.Series(
                        f"{output_col}_proba", probas,
                    ),
                )
            table = chunk.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(
                    out_path, table.schema,
                )
            writer.write_table(table)
            del chunk, x_data, table
    finally:
        if writer is not None:
            writer.close()
    return out_path
