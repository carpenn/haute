"""Core TrainingJob class — orchestrates the full training pipeline."""

from __future__ import annotations

import gc
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from haute._logging import get_logger
from haute.modelling._algorithms import (
    ALGORITHM_REGISTRY,
    IterationCallback,
    _malloc_trim,
    resolve_loss_function,
)
from haute.modelling._metrics import compute_metrics
from haute.modelling._split import SplitConfig, split_mask

logger = get_logger(component="training_job")


@dataclass
class TrainResult:
    """Result of a training job."""

    metrics: dict[str, float]
    feature_importance: list[dict[str, Any]]
    model_path: str
    train_rows: int
    test_rows: int
    features: list[str]
    cat_features: list[str]
    best_iteration: int | None = None
    loss_history: list[dict[str, float]] = field(default_factory=list)
    double_lift: list[dict[str, Any]] = field(default_factory=list)
    shap_summary: list[dict[str, Any]] = field(default_factory=list)
    feature_importance_loss: list[dict[str, Any]] = field(default_factory=list)
    cv_results: dict[str, Any] | None = None
    ave_per_feature: list[dict[str, Any]] = field(default_factory=list)


class TrainingJob:
    """Orchestrates model training: split, fit, evaluate, optionally log to MLflow.

    Parameters
    ----------
    name : str
        Name for the model / training run.
    data : str | pl.DataFrame
        Path to parquet file, or a DataFrame directly.
    target : str
        Target column name.
    weight : str | None
        Optional weight/exposure column name.
    exclude : list[str] | None
        Columns to exclude from features. Everything not in
        {target, weight, *exclude} is automatically a feature.
    algorithm : str
        Algorithm key from ALGORITHM_REGISTRY (default: "catboost").
    task : str
        "regression" or "classification".
    params : dict | None
        Algorithm-specific hyperparameters.
    split : dict | SplitConfig | None
        Split configuration (strategy, test_size, seed, etc.).
    metrics : list[str] | None
        Metrics to compute (default: ["gini", "rmse"]).
    mlflow_experiment : str | None
        MLflow experiment path. If set and mlflow is importable, logs the run.
    model_name : str | None
        Optional MLflow registered model name.
    output_dir : str
        Directory to save the model file.
    """

    def __init__(
        self,
        *,
        name: str,
        data: str | pl.DataFrame,
        target: str,
        weight: str | None = None,
        exclude: list[str] | None = None,
        algorithm: str = "catboost",
        task: str = "regression",
        params: dict[str, Any] | None = None,
        split: dict[str, Any] | SplitConfig | None = None,
        metrics: list[str] | None = None,
        mlflow_experiment: str | None = None,
        model_name: str | None = None,
        output_dir: str = "models",
        loss_function: str | None = None,
        variance_power: float | None = None,
        offset: str | None = None,
        monotone_constraints: dict[str, int] | None = None,
        feature_weights: dict[str, float] | None = None,
        cv_folds: int | None = None,
    ) -> None:
        self.name = name
        self._data = data
        self.target = target
        self.weight = weight
        self.exclude = exclude or []
        self.algorithm = algorithm
        self.task = task
        self.params = params or {}
        self.metrics = metrics or (["gini", "rmse"] if task == "regression" else ["auc", "logloss"])
        self.mlflow_experiment = mlflow_experiment
        self.model_name = model_name
        self.output_dir = output_dir
        self.loss_function = loss_function
        self.variance_power = variance_power
        self.offset = offset
        self.monotone_constraints = monotone_constraints
        self.feature_weights = feature_weights
        self.cv_folds = cv_folds

        # Parse split config
        if isinstance(split, SplitConfig):
            self.split_config = split
        elif isinstance(split, dict):
            self.split_config = SplitConfig(**split)
        else:
            self.split_config = SplitConfig()

    def run(
        self,
        progress: Callable[[str, float], None] | None = None,
        on_iteration: IterationCallback | None = None,
    ) -> TrainResult:
        """Execute the full training pipeline.

        Parameters
        ----------
        progress : callable | None
            Optional callback ``(message, fraction)`` for progress reporting.
        on_iteration : callable | None
            Optional callback ``(iteration, total, metrics_dict)`` called
            after each training iteration.

        Returns
        -------
        TrainResult
            Metrics, feature importances, model path, and split sizes.
        """

        def _report(msg: str, frac: float) -> None:
            if progress:
                progress(msg, frac)

        # 1. Load data — either a parquet path on disk (from route sink)
        #    or a DataFrame (from tests / direct usage).
        _report("Loading data", 0.0)
        import os
        import tempfile

        from haute.modelling._algorithms import _build_pool, _mem_checkpoint

        _mem_checkpoint("training run START")

        _owns_tmp = False  # whether we created the temp parquet ourselves
        data_path: str | None = None

        if isinstance(self._data, str) and self._data.endswith(".parquet"):
            # Route already sunk the LazyFrame to disk — no collect needed
            data_path = self._data
            self._data = None
            _mem_checkpoint(f"using on-disk parquet: {data_path}")
        else:
            # DataFrame or LazyFrame: collect, write to temp parquet, free
            df = self._load_data()
            self._data = None
            _mem_checkpoint(f"data loaded ({len(df):,} rows)")

            _report("Validating columns", 0.05)
            if len(df) == 0:
                raise ValueError("DataFrame is empty — cannot train on zero rows")
            self._validate_columns(df)

            # Drop null targets
            null_count = df[self.target].null_count()
            if null_count is not None and null_count > 0:
                df = df.filter(pl.col(self.target).is_not_null())
                _mem_checkpoint(f"dropped {null_count:,} rows with null target")

            tmp_fd, data_path = tempfile.mkstemp(suffix=".parquet", prefix="haute_split_")
            os.close(tmp_fd)
            _owns_tmp = True
            df.write_parquet(data_path)
            del df
            gc.collect()
            _malloc_trim()
            _mem_checkpoint("wrote temp parquet, freed df")

        # 2. Validate schema from parquet metadata (cheap, no data loaded)
        _report("Validating columns", 0.05)
        import pyarrow.parquet as pq
        _pq_meta = pq.read_metadata(data_path)
        if _pq_meta.num_rows == 0:
            raise ValueError("DataFrame is empty — cannot train on zero rows")
        schema_lf = pl.scan_parquet(data_path)
        schema_df = schema_lf.head(0).collect()
        self._validate_columns(schema_df)

        # 2b. Drop null targets — re-write parquet without nulls if needed
        null_count = (
            pl.scan_parquet(data_path)
            .select(pl.col(self.target).is_null().sum())
            .collect()
            .item()
        )
        if null_count is not None and null_count > 0:
            clean_lf = pl.scan_parquet(data_path).filter(pl.col(self.target).is_not_null())
            tmp_fd2, clean_path = tempfile.mkstemp(suffix=".parquet", prefix="haute_clean_")
            os.close(tmp_fd2)
            try:
                clean_lf.sink_parquet(clean_path)
            except Exception as sink_exc:
                logger.info("sink_streaming_fallback", step="null_clean", reason=str(sink_exc))
                clean_lf.collect(engine="streaming").write_parquet(clean_path)
            # Swap: delete old temp, use new one
            if _owns_tmp:
                os.unlink(data_path)
            data_path = clean_path
            _owns_tmp = True
            _mem_checkpoint(f"dropped {null_count:,} null-target rows")

        # 3. Derive features from schema
        features, cat_features = self._derive_features(schema_df)
        del schema_df, schema_lf
        _report(f"Using {len(features)} features ({len(cat_features)} categorical)", 0.1)

        # 4. Compute split mask and write split parquet
        _report("Splitting data", 0.15)
        _need_cv = bool(self.cv_folds and self.cv_folds > 1)

        # Read row count from parquet metadata (no data loaded)
        total_rows = _pq_meta.num_rows

        # Compute mask — for temporal/group we need a small scan
        mask_df = None
        if self.split_config.strategy in ("temporal", "group"):
            col = self.split_config.date_column or self.split_config.group_column
            mask_df = pl.scan_parquet(data_path).select(col).collect()
        mask = split_mask(total_rows, self.split_config, df=mask_df)
        del mask_df
        n_train = int(mask.sum())
        n_test = total_rows - n_train
        _mem_checkpoint(f"split mask (train={n_train:,} test={n_test:,})")

        # Write split parquet: original data + _is_train column
        tmp_fd3, split_path = tempfile.mkstemp(suffix=".parquet", prefix="haute_split_")
        os.close(tmp_fd3)
        try:
            (
                pl.scan_parquet(data_path)
                .with_columns(mask.to_frame().lazy())
                .sink_parquet(split_path)
            )
        except Exception as sink_exc:
            logger.info("sink_streaming_fallback", step="split_mask", reason=str(sink_exc))
            df_tmp = pl.scan_parquet(data_path).collect(engine="streaming")
            df_tmp.with_columns(mask).write_parquet(split_path)
            del df_tmp
        del mask
        gc.collect()
        _malloc_trim()

        # Free the original temp parquet if we own it
        if _owns_tmp and data_path != split_path:
            os.unlink(data_path)
        data_path = split_path
        _owns_tmp = True
        _mem_checkpoint("wrote split parquet")

        # 5. Look up algorithm
        algo_cls = ALGORITHM_REGISTRY.get(self.algorithm)
        if algo_cls is None:
            raise ValueError(
                f"Unknown algorithm: {self.algorithm}. "
                f"Available: {list(ALGORITHM_REGISTRY.keys())}"
            )
        algo = algo_cls()

        # 5b. Resolve loss function and inject into params
        fit_params = {**self.params}
        resolved_loss = resolve_loss_function(self.loss_function, self.task, self.variance_power)
        if resolved_loss:
            fit_params["loss_function"] = resolved_loss

        # 6. Read train partition, extract labels, free DataFrame, then build pool.
        #    Extracting y/w/baseline first and freeing the full DataFrame
        #    before _build_pool prevents triple-copy (Polars + Pandas + Pool).
        _report("Building training pool", 0.2)
        train_df = (
            pl.scan_parquet(data_path)
            .filter(pl.col("_is_train"))
            .drop("_is_train")
            .collect()
        )
        _mem_checkpoint(f"read train partition ({len(train_df):,} rows)")

        # Extract labels/weights as numpy BEFORE freeing the DataFrame
        train_y = train_df[self.target].cast(pl.Float64).to_numpy()
        train_w = train_df[self.weight].cast(pl.Float64).to_numpy() if self.weight else None
        train_baseline = train_df[self.offset].cast(pl.Float64).to_numpy() if self.offset else None
        # Keep only feature columns, free the full DataFrame
        train_features_df = train_df.select(features)
        del train_df
        gc.collect()
        _malloc_trim()
        _mem_checkpoint("extracted labels, freed train_df")

        train_pool = _build_pool(
            train_features_df, features, cat_features,
            y=train_y, w=train_w, baseline=train_baseline,
        )
        del train_features_df, train_y, train_w, train_baseline
        gc.collect()
        _malloc_trim()
        _mem_checkpoint("train pool built")

        # 7. Read test partition, same extract-then-free pattern
        _report("Building eval pool", 0.25)
        test_df = (
            pl.scan_parquet(data_path)
            .filter(~pl.col("_is_train"))
            .drop("_is_train")
            .collect()
        )
        _mem_checkpoint(f"read test partition ({len(test_df):,} rows)")

        test_y = test_df[self.target].cast(pl.Float64).to_numpy()
        test_w = test_df[self.weight].cast(pl.Float64).to_numpy() if self.weight else None
        test_baseline = test_df[self.offset].cast(pl.Float64).to_numpy() if self.offset else None
        test_features_df = test_df.select(features)
        del test_df
        gc.collect()
        _malloc_trim()
        _mem_checkpoint("extracted labels, freed test_df")

        eval_pool = _build_pool(
            test_features_df, features, cat_features,
            y=test_y, w=test_w, baseline=test_baseline,
        )
        del test_features_df, test_y, test_w, test_baseline
        gc.collect()
        _malloc_trim()
        _mem_checkpoint("eval pool built")

        # 8. Fit with pre-built pools (no DataFrame needed — pools hold data)
        _report("Training model", 0.3)
        fit_result = algo.fit(
            None, features, cat_features,
            self.target, self.weight, fit_params, self.task,
            on_iteration=on_iteration,
            offset=self.offset,
            monotone_constraints=self.monotone_constraints,
            feature_weights=self.feature_weights,
            pool=train_pool,
            eval_pool=eval_pool,
        )
        model = fit_result.model
        _mem_checkpoint("algo.fit() returned")
        del train_pool, eval_pool
        gc.collect()
        _malloc_trim()
        _mem_checkpoint("del pools")

        # 9. Re-read test partition from disk for evaluation
        _report("Evaluating on test set", 0.7)
        test_df = (
            pl.scan_parquet(data_path)
            .filter(~pl.col("_is_train"))
            .drop("_is_train")
            .collect()
        )
        _mem_checkpoint(f"re-read test partition ({len(test_df):,} rows)")
        y_true = test_df[self.target].to_numpy()
        y_pred = algo.predict(model, test_df, features)
        w = test_df[self.weight].to_numpy() if self.weight else None

        # 8. Compute metrics
        _report("Computing metrics", 0.8)
        metrics = compute_metrics(y_true, y_pred, w, self.metrics)

        # 8b. Compute double-lift (actual vs predicted by decile)
        from haute.modelling._metrics import compute_double_lift
        double_lift = compute_double_lift(y_true, y_pred, w)

        # 9. Feature importance (PredictionValuesChange)
        importance = algo.feature_importance(model)

        # 9a. Actual vs Expected per feature (top 15 by importance)
        from haute.modelling._metrics import compute_ave_per_feature
        sorted_features = [fi["feature"] for fi in importance if fi["feature"] in features]
        # Add any features not in importance list (shouldn't happen, but defensive)
        sorted_features += [f for f in features if f not in sorted_features]
        ave_per_feature = compute_ave_per_feature(
            test_df, sorted_features, cat_features, y_true, y_pred, w,
        )

        # 9b. SHAP values + LossFunctionChange importance on test set
        _report("Computing SHAP values", 0.85)
        shap_summary: list[dict[str, float]] = []
        feature_importance_loss: list[dict[str, Any]] = []
        if hasattr(algo, "shap_summary"):
            try:
                shap_summary = algo.shap_summary(model, test_df, features)
            except Exception:
                pass  # SHAP is best-effort, don't fail the run
        if hasattr(algo, "feature_importance_typed"):
            try:
                _test_pool = _build_pool(
                    test_df, features, cat_features, target=self.target,
                )
                feature_importance_loss = algo.feature_importance_typed(
                    model, _test_pool, "LossFunctionChange",
                )
                del _test_pool
            except Exception:
                pass

        # Free test DataFrame — no longer needed after this point
        del test_df
        gc.collect()

        # 9c. Cross-validation (in addition to the normal train)
        cv_results: dict[str, Any] | None = None
        if _need_cv and hasattr(algo, "cross_validate"):
            _report("Running cross-validation", 0.88)
            try:
                _cv_df = (
                    pl.scan_parquet(data_path)
                    .drop("_is_train")
                    .collect()
                )
                cv_results = algo.cross_validate(
                    _cv_df, features, cat_features,
                    self.target, self.weight, fit_params, self.task,
                    n_folds=self.cv_folds,
                )
                del _cv_df
                gc.collect()
            except Exception:
                pass  # CV is best-effort
        # Clean up split parquet (kept alive for test re-read + CV)
        if _owns_tmp and os.path.exists(data_path):
            os.unlink(data_path)

        # 10. Save model
        _report("Saving model", 0.9)
        ext = ".cbm" if self.algorithm == "catboost" else ".model"
        model_path = Path(self.output_dir) / f"{self.name}{ext}"
        algo.save(model, model_path)

        # 11. Optionally log to MLflow
        if self.mlflow_experiment:
            self._log_to_mlflow(
                metrics, importance, str(model_path),
                shap_summary=shap_summary,
                feature_importance_loss=feature_importance_loss,
                cv_results=cv_results,
                double_lift=double_lift,
                loss_history=fit_result.loss_history,
                ave_per_feature=ave_per_feature,
                algorithm=self.algorithm,
                task=self.task,
                train_rows=n_train,
                test_rows=n_test,
                best_iteration=fit_result.best_iteration,
                features=features,
                split_config=self.split_config.__dict__,
            )

        _report("Done", 1.0)

        return TrainResult(
            metrics=metrics,
            feature_importance=importance,
            model_path=str(model_path),
            train_rows=n_train,
            test_rows=n_test,
            features=features,
            cat_features=cat_features,
            best_iteration=fit_result.best_iteration,
            loss_history=fit_result.loss_history,
            double_lift=double_lift,
            shap_summary=shap_summary,
            feature_importance_loss=feature_importance_loss,
            cv_results=cv_results,
            ave_per_feature=ave_per_feature,
        )

    def _load_data(self) -> pl.DataFrame:
        """Load data from path or use directly if already a DataFrame."""
        if isinstance(self._data, pl.DataFrame):
            return self._data
        if isinstance(self._data, pl.LazyFrame):
            return self._data.collect()
        from haute._io import read_source

        path = Path(self._data)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")
        return read_source(str(path)).collect()

    def _validate_columns(self, df: pl.DataFrame) -> None:
        """Validate that required columns exist in the DataFrame."""
        if self.target not in df.columns:
            raise ValueError(
                f"Target column '{self.target}' not found. "
                f"Available: {df.columns}"
            )
        if self.weight and self.weight not in df.columns:
            raise ValueError(
                f"Weight column '{self.weight}' not found. "
                f"Available: {df.columns}"
            )
        if self.offset and self.offset not in df.columns:
            raise ValueError(
                f"Offset column '{self.offset}' not found. "
                f"Available: {df.columns}"
            )
        for col in self.exclude:
            if col not in df.columns:
                raise ValueError(
                    f"Exclude column '{col}' not found. "
                    f"Available: {df.columns}"
                )

    def _derive_features(self, df: pl.DataFrame) -> tuple[list[str], list[str]]:
        """Derive feature list: all columns minus {target, weight, *exclude}.

        Also detects categorical features from Polars dtype.
        """
        non_features = {self.target}
        if self.weight:
            non_features.add(self.weight)
        if self.offset:
            non_features.add(self.offset)
        non_features.update(self.exclude)

        features = [c for c in df.columns if c not in non_features]
        if not features:
            raise ValueError(
                "No feature columns remaining after excluding "
                f"{non_features}. Check your target/weight/exclude settings."
            )

        # Detect categorical features from Polars dtype
        cat_features = []
        for col in features:
            dtype = df[col].dtype
            if dtype in (pl.Utf8, pl.Categorical, pl.String):
                cat_features.append(col)

        return features, cat_features

    def _log_to_mlflow(
        self,
        metrics: dict[str, float],
        importance: list[dict[str, Any]],
        model_path: str,
        shap_summary: list[dict[str, float]] | None = None,
        feature_importance_loss: list[dict[str, Any]] | None = None,
        cv_results: dict[str, Any] | None = None,
        double_lift: list[dict[str, Any]] | None = None,
        loss_history: list[dict[str, float]] | None = None,
        ave_per_feature: list[dict[str, Any]] | None = None,
        algorithm: str = "",
        task: str = "",
        train_rows: int = 0,
        test_rows: int = 0,
        best_iteration: int | None = None,
        features: list[str] | None = None,
        split_config: dict[str, Any] | None = None,
    ) -> None:
        """Log training run to MLflow (conditional import).

        Delegates to the standalone ``log_experiment()`` function so the
        same logic is reused by the "Log to MLflow" button in the UI.
        """
        try:
            from haute.modelling._mlflow_log import log_experiment
        except ImportError:
            return

        if not self.mlflow_experiment:
            return

        log_experiment(
            experiment_name=self.mlflow_experiment,
            run_name=self.name,
            metrics=metrics,
            params={
                "algorithm": self.algorithm,
                "task": self.task,
                "target": self.target,
                "weight": self.weight or "",
                "split_strategy": self.split_config.strategy,
                "test_size": self.split_config.test_size,
                **{f"param_{k}": v for k, v in self.params.items()},
            },
            model_path=model_path,
            model_name=self.model_name,
            shap_summary=shap_summary,
            feature_importance_loss=feature_importance_loss,
            cv_results=cv_results,
            feature_importance=importance,
            double_lift=double_lift,
            loss_history=loss_history,
            ave_per_feature=ave_per_feature,
            algorithm=algorithm or self.algorithm,
            task=task or self.task,
            train_rows=train_rows,
            test_rows=test_rows,
            best_iteration=best_iteration,
            features=features,
            split_config=split_config,
        )
