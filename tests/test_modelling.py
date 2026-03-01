"""Tests for haute.modelling — TrainingJob, algorithms, metrics, splits."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from haute.modelling._algorithms import ALGORITHM_REGISTRY, CatBoostAlgorithm, FitResult, resolve_loss_function
from haute.modelling._metrics import compute_double_lift, compute_metrics
from haute.modelling._split import SplitConfig, split_data, split_mask
from haute.modelling._training_job import TrainResult, TrainingJob


# ---------------------------------------------------------------------------
# SplitConfig validation
# ---------------------------------------------------------------------------


class TestSplitConfig:
    def test_invalid_test_size_zero(self):
        with pytest.raises(ValueError, match="test_size"):
            SplitConfig(test_size=0)

    def test_invalid_test_size_one(self):
        with pytest.raises(ValueError, match="test_size"):
            SplitConfig(test_size=1.0)

    def test_temporal_requires_date_column(self):
        with pytest.raises(ValueError, match="date_column"):
            SplitConfig(strategy="temporal", cutoff_date="2024-01-01")

    def test_temporal_requires_cutoff_date(self):
        with pytest.raises(ValueError, match="cutoff_date"):
            SplitConfig(strategy="temporal", date_column="date")

    def test_group_requires_group_column(self):
        with pytest.raises(ValueError, match="group_column"):
            SplitConfig(strategy="group")


# ---------------------------------------------------------------------------
# split_data
# ---------------------------------------------------------------------------


class TestSplitData:
    @pytest.fixture()
    def sample_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "x": list(range(100)),
            "y": [float(i % 3) for i in range(100)],
            "group": [f"g{i % 5}" for i in range(100)],
            "date": [f"2024-{(i % 12) + 1:02d}-15" for i in range(100)],
        })

    def test_empty_dataframe_raises(self):
        df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
        with pytest.raises(ValueError, match="empty"):
            split_data(df, SplitConfig())

    def test_random_split_proportions(self, sample_df):
        train, test = split_data(sample_df, SplitConfig(test_size=0.2, seed=42))
        assert len(train) == 80
        assert len(test) == 20
        # No row overlap
        train_idx = set(train["x"].to_list())
        test_idx = set(test["x"].to_list())
        assert train_idx & test_idx == set()

    def test_random_split_seed_reproducible(self, sample_df):
        t1, _ = split_data(sample_df, SplitConfig(test_size=0.2, seed=42))
        t2, _ = split_data(sample_df, SplitConfig(test_size=0.2, seed=42))
        assert t1["x"].to_list() == t2["x"].to_list()

    def test_random_split_different_seed(self, sample_df):
        t1, _ = split_data(sample_df, SplitConfig(test_size=0.2, seed=42))
        t2, _ = split_data(sample_df, SplitConfig(test_size=0.2, seed=99))
        assert t1["x"].to_list() != t2["x"].to_list()

    def test_temporal_split(self, sample_df):
        config = SplitConfig(
            strategy="temporal",
            date_column="date",
            cutoff_date="2024-07-01",
        )
        train, test = split_data(sample_df, config)
        assert len(train) + len(test) == len(sample_df)
        # All train dates < cutoff, all test dates >= cutoff
        assert all(d < "2024-07-01" for d in train["date"].to_list())
        assert all(d >= "2024-07-01" for d in test["date"].to_list())

    def test_temporal_split_missing_column(self, sample_df):
        config = SplitConfig(
            strategy="temporal",
            date_column="nonexistent",
            cutoff_date="2024-07-01",
        )
        with pytest.raises(ValueError, match="not found"):
            split_data(sample_df, config)

    def test_group_split(self, sample_df):
        config = SplitConfig(strategy="group", group_column="group", test_size=0.3, seed=42)
        train, test = split_data(sample_df, config)
        assert len(train) + len(test) == len(sample_df)
        # All rows of a group go to the same set
        train_groups = set(train["group"].unique().to_list())
        test_groups = set(test["group"].unique().to_list())
        assert train_groups & test_groups == set()

    def test_group_split_missing_column(self, sample_df):
        config = SplitConfig(strategy="group", group_column="nonexistent")
        with pytest.raises(ValueError, match="not found"):
            split_data(sample_df, config)


# ---------------------------------------------------------------------------
# split_mask (Boolean mask variant — no DataFrame copies)
# ---------------------------------------------------------------------------


class TestSplitMask:
    def test_random_mask_correct_ratio(self):
        n = 1000
        mask = split_mask(n, SplitConfig(test_size=0.2, seed=42))
        assert len(mask) == n
        assert mask.dtype == pl.Boolean
        train_n = mask.sum()
        assert 750 < train_n < 850  # ~80% train ± tolerance

    def test_random_mask_deterministic(self):
        cfg = SplitConfig(test_size=0.2, seed=42)
        m1 = split_mask(500, cfg)
        m2 = split_mask(500, cfg)
        assert m1.to_list() == m2.to_list()

    def test_random_mask_different_seed(self):
        m1 = split_mask(500, SplitConfig(test_size=0.2, seed=42))
        m2 = split_mask(500, SplitConfig(test_size=0.2, seed=99))
        assert m1.to_list() != m2.to_list()

    def test_temporal_mask_splits_by_date(self):
        df = pl.DataFrame({"date": ["2024-01-01", "2024-06-15", "2024-12-31"]})
        cfg = SplitConfig(
            strategy="temporal", date_column="date", cutoff_date="2024-07-01",
        )
        mask = split_mask(len(df), cfg, df=df)
        assert mask.to_list() == [True, True, False]

    def test_temporal_mask_missing_df_raises(self):
        cfg = SplitConfig(
            strategy="temporal", date_column="date", cutoff_date="2024-07-01",
        )
        with pytest.raises(ValueError, match="requires df"):
            split_mask(10, cfg, df=None)

    def test_group_mask_keeps_groups_intact(self):
        df = pl.DataFrame({
            "group": [f"g{i % 5}" for i in range(100)],
        })
        cfg = SplitConfig(strategy="group", group_column="group", test_size=0.3, seed=42)
        mask = split_mask(len(df), cfg, df=df)
        # Each group should be entirely in train or entirely in test
        labeled = df.with_columns(mask)
        for group_val in df["group"].unique().to_list():
            group_masks = labeled.filter(pl.col("group") == group_val)["_is_train"].to_list()
            assert len(set(group_masks)) == 1, f"Group {group_val} split across train/test"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            split_mask(0, SplitConfig())


# ---------------------------------------------------------------------------
# compute_metrics
# ---------------------------------------------------------------------------


class TestComputeMetrics:
    def test_unknown_metric_raises(self):
        with pytest.raises(ValueError, match="Unknown metric"):
            compute_metrics(np.array([1]), np.array([1]), None, ["nonexistent"])

    @pytest.mark.parametrize(
        "metric, y_true, y_pred, expected, tolerance",
        [
            pytest.param("rmse", [1.0, 2.0, 3.0], [1.0, 2.0, 3.0], 0.0, 1e-10, id="rmse_perfect"),
            pytest.param("rmse", [1.0, 2.0, 3.0], [1.0, 2.0, 4.0], None, None, id="rmse_known"),
            pytest.param("mae", [1.0, 2.0, 3.0], [1.5, 2.5, 3.5], 0.5, None, id="mae_known"),
            pytest.param("mse", [1.0, 2.0, 3.0], [2.0, 2.0, 2.0], None, None, id="mse_known"),
            pytest.param("r2", [1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0], 1.0, 1e-10, id="r2_perfect"),
        ],
    )
    def test_core_metric(self, metric, y_true, y_pred, expected, tolerance):
        yt = np.array(y_true)
        yp = np.array(y_pred)
        result = compute_metrics(yt, yp, None, [metric])
        if expected is None:
            # Compute expected from numpy for known-value tests
            if metric == "rmse":
                expected = np.sqrt(np.mean((yt - yp) ** 2))
            elif metric == "mse":
                expected = np.mean((yt - yp) ** 2)
        approx_kwargs = {"abs": tolerance} if tolerance else {}
        assert result[metric] == pytest.approx(expected, **approx_kwargs)

    def test_gini_perfect_ranking(self):
        """Perfect ranking gives Gini = 1.0."""
        y_true = np.array([0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 2.0])
        y_pred = y_true.copy()  # perfect prediction
        result = compute_metrics(y_true, y_pred, None, ["gini"])
        assert result["gini"] == pytest.approx(1.0, abs=0.01)

    def test_gini_random(self):
        """Random predictions give Gini close to 0."""
        rng = np.random.RandomState(42)
        y_true = rng.choice([0, 1], size=1000)
        y_pred = rng.random(1000)
        result = compute_metrics(y_true, y_pred, None, ["gini"])
        assert abs(result["gini"]) < 0.15

    def test_weighted_rmse(self):
        y_true = np.array([1.0, 2.0, 3.0])
        y_pred = np.array([1.0, 2.0, 4.0])
        weights = np.array([1.0, 1.0, 2.0])
        result = compute_metrics(y_true, y_pred, weights, ["rmse"])
        expected = np.sqrt(np.average((y_true - y_pred) ** 2, weights=weights))
        assert result["rmse"] == pytest.approx(expected)

    def test_multiple_metrics(self):
        y = np.array([1.0, 2.0, 3.0])
        result = compute_metrics(y, y, None, ["rmse", "mae", "r2"])
        assert set(result.keys()) == {"rmse", "mae", "r2"}


# ---------------------------------------------------------------------------
# CatBoostAlgorithm
# ---------------------------------------------------------------------------


class TestCatBoostAlgorithm:
    @pytest.fixture()
    def train_data(self) -> pl.DataFrame:
        rng = np.random.RandomState(42)
        n = 200
        return pl.DataFrame({
            "x1": rng.randn(n),
            "x2": rng.randn(n),
            "cat1": rng.choice(["a", "b", "c"], n),
            "target": rng.randn(n),
            "weight": np.ones(n),
        })

    def test_algorithm_registry(self):
        assert "catboost" in ALGORITHM_REGISTRY
        assert ALGORITHM_REGISTRY["catboost"] is CatBoostAlgorithm

    def test_fit_predict(self, train_data):
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            train_data,
            features=["x1", "x2", "cat1"],
            cat_features=["cat1"],
            target="target",
            weight="weight",
            params={"iterations": 10, "depth": 3},
            task="regression",
        )
        assert isinstance(fit_result, FitResult)
        preds = algo.predict(fit_result.model, train_data, ["x1", "x2", "cat1"])
        assert len(preds) == len(train_data)
        assert isinstance(preds, np.ndarray)

    def test_feature_importance(self, train_data):
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            train_data,
            features=["x1", "x2", "cat1"],
            cat_features=["cat1"],
            target="target",
            weight=None,
            params={"iterations": 10, "depth": 3},
            task="regression",
        )
        importance = algo.feature_importance(fit_result.model)
        assert len(importance) == 3
        assert all("feature" in fi and "importance" in fi for fi in importance)

    def test_save_load(self, train_data, tmp_path):
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            train_data,
            features=["x1", "x2"],
            cat_features=[],
            target="target",
            weight=None,
            params={"iterations": 5},
            task="regression",
        )
        model = fit_result.model
        model_path = tmp_path / "test.cbm"
        algo.save(model, model_path)
        assert model_path.exists()

        # Load and predict
        from catboost import CatBoostRegressor

        loaded = CatBoostRegressor()
        loaded.load_model(str(model_path))
        X = train_data.select(["x1", "x2"]).to_pandas()
        preds_orig = model.predict(X)
        preds_loaded = loaded.predict(X)
        np.testing.assert_array_almost_equal(preds_orig, preds_loaded)

    def test_classification(self, train_data):
        # Add binary target
        df = train_data.with_columns(
            (pl.col("target") > 0).cast(pl.Int32).alias("binary_target"),
        )
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            df,
            features=["x1", "x2"],
            cat_features=[],
            target="binary_target",
            weight=None,
            params={"iterations": 10},
            task="classification",
        )
        preds = algo.predict(fit_result.model, df, ["x1", "x2"])
        assert len(preds) == len(df)

    def test_early_stopping_with_eval_df(self, train_data):
        """When eval_df is provided with early stopping, best_iteration is set."""
        algo = CatBoostAlgorithm()
        # Split data to get an eval set
        train = train_data[:150]
        eval_data = train_data[150:]
        fit_result = algo.fit(
            train,
            features=["x1", "x2", "cat1"],
            cat_features=["cat1"],
            target="target",
            weight=None,
            params={"iterations": 10000, "depth": 3, "early_stopping_rounds": 5},
            task="regression",
            eval_df=eval_data,
        )
        assert isinstance(fit_result, FitResult)
        # Should stop early — best_iteration should be much less than 10000
        assert fit_result.best_iteration is not None
        assert fit_result.best_iteration < 10000

    def test_loss_history_collected(self, train_data):
        """Loss history is collected even without eval_df."""
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            train_data,
            features=["x1", "x2"],
            cat_features=[],
            target="target",
            weight=None,
            params={"iterations": 10},
            task="regression",
        )
        assert len(fit_result.loss_history) == 10
        assert "iteration" in fit_result.loss_history[0]
        assert any(k.startswith("train_") for k in fit_result.loss_history[0])

    def test_eval_df_loss_history_has_eval_keys(self, train_data):
        """With eval_df, loss history includes eval_ prefixed keys."""
        algo = CatBoostAlgorithm()
        train = train_data[:150]
        eval_data = train_data[150:]
        fit_result = algo.fit(
            train,
            features=["x1", "x2"],
            cat_features=[],
            target="target",
            weight=None,
            params={"iterations": 10, "depth": 3},
            task="regression",
            eval_df=eval_data,
        )
        assert len(fit_result.loss_history) > 0
        first = fit_result.loss_history[0]
        assert any(k.startswith("eval_") for k in first)


# ---------------------------------------------------------------------------
# TrainingJob
# ---------------------------------------------------------------------------


class TestTrainingJob:
    @pytest.fixture()
    def synth_data(self) -> pl.DataFrame:
        rng = np.random.RandomState(42)
        n = 100
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        return pl.DataFrame({
            "IDpol": list(range(n)),
            "x1": x1,
            "x2": x2,
            "Exposure": np.ones(n),
            "ClaimCount": (x1 + x2 + rng.randn(n) * 0.5).clip(0),
        })

    def test_basic_training(self, synth_data, tmp_path):
        job = TrainingJob(
            name="test_model",
            data=synth_data,
            target="ClaimCount",
            weight="Exposure",
            exclude=["IDpol"],
            params={"iterations": 10, "depth": 3},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert isinstance(result, TrainResult)
        assert "gini" in result.metrics
        assert "rmse" in result.metrics
        assert result.train_rows + result.test_rows == len(synth_data)
        assert len(result.features) == 2  # x1, x2
        assert (tmp_path / "test_model.cbm").exists()

    def test_feature_derivation(self, synth_data, tmp_path):
        """Features = all columns - target - weight - exclude."""
        job = TrainingJob(
            name="feat_test",
            data=synth_data,
            target="ClaimCount",
            weight="Exposure",
            exclude=["IDpol"],
            params={"iterations": 5},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert set(result.features) == {"x1", "x2"}

    def test_missing_target_raises(self, synth_data, tmp_path):
        job = TrainingJob(
            name="bad",
            data=synth_data,
            target="nonexistent",
            output_dir=str(tmp_path),
        )
        with pytest.raises(ValueError, match="Target column"):
            job.run()

    def test_empty_dataframe_raises(self, tmp_path):
        df = pl.DataFrame({
            "x": pl.Series([], dtype=pl.Float64),
            "y": pl.Series([], dtype=pl.Float64),
        })
        job = TrainingJob(
            name="empty",
            data=df,
            target="y",
            output_dir=str(tmp_path),
        )
        with pytest.raises(ValueError, match="empty"):
            job.run()

    def test_with_weight_column(self, synth_data, tmp_path):
        job = TrainingJob(
            name="weighted",
            data=synth_data,
            target="ClaimCount",
            weight="Exposure",
            exclude=["IDpol"],
            params={"iterations": 5},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.metrics

    def test_classification_task(self, tmp_path):
        rng = np.random.RandomState(42)
        n = 100
        df = pl.DataFrame({
            "x1": rng.randn(n),
            "x2": rng.randn(n),
            "label": rng.choice([0, 1], n),
        })
        job = TrainingJob(
            name="cls",
            data=df,
            target="label",
            task="classification",
            params={"iterations": 10},
            metrics=["auc", "logloss"],
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert "auc" in result.metrics
        assert "logloss" in result.metrics

    def test_progress_callback(self, synth_data, tmp_path):
        messages: list[tuple[str, float]] = []

        def _progress(msg: str, frac: float) -> None:
            messages.append((msg, frac))

        job = TrainingJob(
            name="progress",
            data=synth_data,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 5},
            output_dir=str(tmp_path),
        )
        job.run(progress=_progress)
        assert len(messages) > 0
        assert messages[-1][1] == 1.0

    def test_split_config_from_dict(self, synth_data, tmp_path):
        job = TrainingJob(
            name="split_dict",
            data=synth_data,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 5},
            split={"strategy": "random", "test_size": 0.3, "seed": 99},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.test_rows == 30

    def test_unknown_algorithm_raises(self, synth_data, tmp_path):
        job = TrainingJob(
            name="bad_algo",
            data=synth_data,
            target="ClaimCount",
            algorithm="xgboost",
            output_dir=str(tmp_path),
        )
        with pytest.raises(ValueError, match="Unknown algorithm"):
            job.run()

    def test_data_from_lazyframe(self, synth_data, tmp_path):
        lf = synth_data.lazy()
        job = TrainingJob(
            name="lazy",
            data=lf,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 5},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.train_rows > 0

    def test_poisson_loss(self, synth_data, tmp_path):
        job = TrainingJob(
            name="poisson",
            data=synth_data,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 10},
            loss_function="Poisson",
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.metrics

    def test_tweedie_loss(self, synth_data, tmp_path):
        job = TrainingJob(
            name="tweedie",
            data=synth_data,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 10},
            loss_function="Tweedie",
            variance_power=1.5,
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.metrics

    def test_offset_column(self, synth_data, tmp_path):
        # Add a log-exposure offset column
        data = synth_data.with_columns(pl.col("Exposure").log().alias("log_exposure"))
        job = TrainingJob(
            name="offset",
            data=data,
            target="ClaimCount",
            weight="Exposure",
            exclude=["IDpol"],
            offset="log_exposure",
            params={"iterations": 10},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.metrics
        # Offset column should not be in features
        assert "log_exposure" not in result.features

    def test_double_lift_computed(self, synth_data, tmp_path):
        job = TrainingJob(
            name="dlift",
            data=synth_data,
            target="ClaimCount",
            exclude=["IDpol", "Exposure"],
            params={"iterations": 10},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert len(result.double_lift) == 10
        assert all(k in result.double_lift[0] for k in ("decile", "actual", "predicted", "count"))


# ---------------------------------------------------------------------------
# Loss function resolution
# ---------------------------------------------------------------------------


class TestResolveLossFunction:
    def test_none_returns_none(self):
        assert resolve_loss_function(None, "regression") is None
        assert resolve_loss_function("", "regression") is None

    def test_regression_losses(self):
        assert resolve_loss_function("RMSE", "regression") == "RMSE"
        assert resolve_loss_function("MAE", "regression") == "MAE"
        assert resolve_loss_function("Poisson", "regression") == "Poisson"

    def test_tweedie_includes_variance_power(self):
        result = resolve_loss_function("Tweedie", "regression", 1.5)
        assert result == "Tweedie:variance_power=1.5"

    def test_tweedie_default_variance_power(self):
        result = resolve_loss_function("Tweedie", "regression")
        assert result == "Tweedie:variance_power=1.5"

    def test_classification_losses(self):
        assert resolve_loss_function("Logloss", "classification") == "Logloss"
        assert resolve_loss_function("CrossEntropy", "classification") == "CrossEntropy"

    def test_invalid_loss_for_task(self):
        with pytest.raises(ValueError, match="not valid"):
            resolve_loss_function("Poisson", "classification")
        with pytest.raises(ValueError, match="not valid"):
            resolve_loss_function("Logloss", "regression")


# ---------------------------------------------------------------------------
# Double-lift
# ---------------------------------------------------------------------------


class TestDoubleLift:
    def test_basic_double_lift(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
        y_pred = y_true * 1.1
        result = compute_double_lift(y_true, y_pred, n_bins=5)
        assert len(result) == 5
        assert result[0]["decile"] == 1
        # Lowest decile predictions should be lowest
        assert result[0]["predicted"] < result[-1]["predicted"]

    def test_empty_arrays(self):
        result = compute_double_lift(np.array([]), np.array([]))
        assert result == []

    def test_weighted_double_lift(self):
        y_true = np.arange(20, dtype=float)
        y_pred = y_true + 1
        w = np.ones(20)
        w[:10] = 2.0
        result = compute_double_lift(y_true, y_pred, w, n_bins=4)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# Deviance metrics
# ---------------------------------------------------------------------------


class TestDevianceMetrics:
    def test_poisson_deviance(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        y_pred = y_true.copy()
        result = compute_metrics(y_true, y_pred, None, ["poisson_deviance"])
        assert result["poisson_deviance"] == pytest.approx(0.0, abs=1e-8)

    def test_poisson_deviance_nonzero(self):
        y_true = np.array([1.0, 2.0, 3.0])
        y_pred = np.array([2.0, 2.0, 2.0])
        result = compute_metrics(y_true, y_pred, None, ["poisson_deviance"])
        assert result["poisson_deviance"] > 0

    def test_tweedie_deviance(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0])
        y_pred = y_true.copy()
        result = compute_metrics(y_true, y_pred, None, ["tweedie_deviance"])
        assert result["tweedie_deviance"] == pytest.approx(0.0, abs=1e-6)


# ---------------------------------------------------------------------------
# Monotonic Constraints
# ---------------------------------------------------------------------------


class TestMonotonicConstraints:
    def test_monotone_constraint_training(self):
        """Training with monotone_constraints should succeed."""
        rng = np.random.RandomState(42)
        n = 200
        x1 = rng.randn(n)
        df = pl.DataFrame({
            "x1": x1,
            "x2": rng.randn(n),
            "y": x1 + rng.randn(n) * 0.1,
        })
        job = TrainingJob(
            name="mono",
            data=df,
            target="y",
            params={"iterations": 20, "depth": 3},
            monotone_constraints={"x1": 1},
            output_dir="/tmp/test_mono",
        )
        result = job.run()
        assert result.metrics

    def test_monotone_constraint_decreasing(self):
        """With monotone_constraints x1=-1, predictions must decrease with x1."""
        rng = np.random.RandomState(42)
        n = 500
        x1 = rng.randn(n)
        df = pl.DataFrame({
            "x1": x1,
            "x2": rng.randn(n),
            "y": -x1 * 2 + rng.randn(n) * 0.1,
        })
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            df, features=["x1", "x2"], cat_features=[], target="y", weight=None,
            params={"iterations": 50, "depth": 4}, task="regression",
            monotone_constraints={"x1": -1},
        )
        test_df = pl.DataFrame({"x1": np.linspace(-3, 3, 20), "x2": np.zeros(20)})
        preds = algo.predict(fit_result.model, test_df, ["x1", "x2"])
        for i in range(1, len(preds)):
            assert preds[i] <= preds[i - 1] + 1e-6

    def test_monotone_constraint_enforced(self):
        """With monotone_constraints x1=+1, predictions must increase with x1."""
        rng = np.random.RandomState(42)
        n = 500
        x1 = rng.randn(n)
        df = pl.DataFrame({
            "x1": x1,
            "x2": rng.randn(n),
            "y": x1 * 2 + rng.randn(n) * 0.1,
        })
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            df, features=["x1", "x2"], cat_features=[], target="y", weight=None,
            params={"iterations": 50, "depth": 4}, task="regression",
            monotone_constraints={"x1": 1},
        )
        # Predict on a grid varying x1 with x2 fixed
        test_df = pl.DataFrame({"x1": np.linspace(-3, 3, 20), "x2": np.zeros(20)})
        preds = algo.predict(fit_result.model, test_df, ["x1", "x2"])
        # Predictions should be non-decreasing
        for i in range(1, len(preds)):
            assert preds[i] >= preds[i - 1] - 1e-6


# ---------------------------------------------------------------------------
# SHAP Values + Feature Analysis
# ---------------------------------------------------------------------------


class TestSHAP:
    @pytest.fixture()
    def trained_model(self):
        rng = np.random.RandomState(42)
        n = 200
        x1 = rng.randn(n)
        x2 = rng.randn(n)
        df = pl.DataFrame({
            "x1": x1,
            "x2": x2,
            "y": x1 * 2 + x2 + rng.randn(n) * 0.1,
        })
        algo = CatBoostAlgorithm()
        fit_result = algo.fit(
            df, features=["x1", "x2"], cat_features=[], target="y", weight=None,
            params={"iterations": 30, "depth": 4}, task="regression",
        )
        return algo, fit_result.model, df

    def test_shap_summary(self, trained_model):
        algo, model, df = trained_model
        summary = algo.shap_summary(model, df, ["x1", "x2"])
        assert len(summary) == 2
        assert "feature" in summary[0]
        assert "mean_abs_shap" in summary[0]
        # x1 has 2x coefficient so should have higher SHAP
        x1_shap = next(s for s in summary if s["feature"] == "x1")
        x2_shap = next(s for s in summary if s["feature"] == "x2")
        assert x1_shap["mean_abs_shap"] > x2_shap["mean_abs_shap"]

    def test_shap_summary_subsamples(self, trained_model):
        algo, model, df = trained_model
        summary = algo.shap_summary(model, df, ["x1", "x2"], max_rows=50)
        assert len(summary) == 2

    def test_feature_importance_typed(self, trained_model):
        from catboost import Pool
        algo, model, df = trained_model
        X = df.select(["x1", "x2"]).to_pandas()
        y = df["y"].to_numpy()
        pool = Pool(data=X, label=y)
        loss_imp = algo.feature_importance_typed(model, pool, "LossFunctionChange")
        assert len(loss_imp) == 2
        assert all("feature" in fi and "importance" in fi for fi in loss_imp)

    def test_training_job_includes_shap(self, tmp_path):
        rng = np.random.RandomState(42)
        n = 200
        df = pl.DataFrame({
            "x1": rng.randn(n),
            "x2": rng.randn(n),
            "y": rng.randn(n),
        })
        job = TrainingJob(
            name="shap_test",
            data=df,
            target="y",
            params={"iterations": 10},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert len(result.shap_summary) == 2
        assert len(result.feature_importance_loss) == 2


# ---------------------------------------------------------------------------
# Cross-Validation
# ---------------------------------------------------------------------------


class TestCrossValidation:
    def test_cv_returns_results(self):
        rng = np.random.RandomState(42)
        n = 200
        df = pl.DataFrame({
            "x1": rng.randn(n),
            "x2": rng.randn(n),
            "y": rng.randn(n),
        })
        algo = CatBoostAlgorithm()
        cv_results = algo.cross_validate(
            df, features=["x1", "x2"], cat_features=[], target="y", weight=None,
            params={"iterations": 10, "depth": 3}, task="regression", n_folds=3,
        )
        assert "mean_metrics" in cv_results
        assert "std_metrics" in cv_results
        assert cv_results["n_folds"] == 3
        assert len(cv_results["mean_metrics"]) > 0

    def test_training_job_with_cv(self, tmp_path):
        rng = np.random.RandomState(42)
        n = 200
        df = pl.DataFrame({
            "x1": rng.randn(n),
            "x2": rng.randn(n),
            "y": rng.randn(n),
        })
        job = TrainingJob(
            name="cv_test",
            data=df,
            target="y",
            params={"iterations": 10},
            cv_folds=3,
            output_dir=str(tmp_path),
        )
        result = job.run()
        # CV should be computed
        assert result.cv_results is not None
        assert result.cv_results["n_folds"] == 3
        assert len(result.cv_results["mean_metrics"]) > 0
        # Normal model should still be trained
        assert result.metrics
        assert result.train_rows > 0

    def test_training_job_without_cv(self, tmp_path):
        rng = np.random.RandomState(42)
        n = 100
        df = pl.DataFrame({"x1": rng.randn(n), "y": rng.randn(n)})
        job = TrainingJob(
            name="no_cv",
            data=df,
            target="y",
            params={"iterations": 5},
            output_dir=str(tmp_path),
        )
        result = job.run()
        assert result.cv_results is None
