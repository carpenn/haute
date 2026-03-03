"""Tests for executor builder functions — the _build_* dispatch table.

Covers the simpler node builders: constant, banding, scenario_expander,
api_input, and output.  Uses the same helpers from conftest.py as the
existing test_executor.py.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from haute.executor import _build_node_fn
from tests.conftest import make_node as _n

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build(
    node_type: str, config: dict, label: str = "test_node",
    source_names: list[str] | None = None,
):
    """Build a node function via _build_node_fn and return (func_name, fn, is_source)."""
    node = _n({
        "id": "n1",
        "data": {"label": label, "nodeType": node_type, "config": config},
    })
    return _build_node_fn(node, source_names=source_names or [])


# ---------------------------------------------------------------------------
# _build_constant
# ---------------------------------------------------------------------------


class TestBuildConstant:
    """Tests for the constant node builder."""

    def test_single_numeric_value(self) -> None:
        func_name, fn, is_source = _build(
            "constant",
            {"values": [{"name": "rate", "value": "0.05"}]},
            label="MyRate",
        )
        assert func_name == "MyRate"
        assert is_source is True
        result = fn().collect()
        assert result.shape == (1, 1)
        assert result["rate"].to_list() == [0.05]

    def test_multiple_values(self) -> None:
        _, fn, _ = _build(
            "constant",
            {"values": [
                {"name": "a", "value": "10"},
                {"name": "b", "value": "20.5"},
            ]},
        )
        result = fn().collect()
        assert result.shape == (1, 2)
        assert result["a"].to_list() == [10.0]
        assert result["b"].to_list() == [20.5]

    def test_string_value_not_numeric(self) -> None:
        _, fn, _ = _build(
            "constant",
            {"values": [{"name": "region", "value": "north"}]},
        )
        result = fn().collect()
        assert result["region"].to_list() == ["north"]

    def test_empty_values_gives_default(self) -> None:
        _, fn, _ = _build("constant", {"values": []})
        result = fn().collect()
        assert "constant" in result.columns
        assert result["constant"].to_list() == [0]

    def test_none_values_gives_default(self) -> None:
        _, fn, _ = _build("constant", {"values": None})
        result = fn().collect()
        assert "constant" in result.columns
        assert result["constant"].to_list() == [0]

    def test_missing_values_key_gives_default(self) -> None:
        _, fn, _ = _build("constant", {})
        result = fn().collect()
        assert "constant" in result.columns

    def test_value_with_empty_name_skipped(self) -> None:
        _, fn, _ = _build(
            "constant",
            {"values": [{"name": "", "value": "5"}, {"name": "x", "value": "1"}]},
        )
        result = fn().collect()
        assert "x" in result.columns
        assert result.shape == (1, 1)  # empty name skipped

    def test_mixed_numeric_and_string(self) -> None:
        _, fn, _ = _build(
            "constant",
            {"values": [
                {"name": "count", "value": "42"},
                {"name": "label", "value": "hello"},
            ]},
        )
        result = fn().collect()
        assert result["count"].to_list() == [42.0]
        assert result["label"].to_list() == ["hello"]

    def test_integer_value_stored_as_float(self) -> None:
        _, fn, _ = _build(
            "constant",
            {"values": [{"name": "n", "value": "100"}]},
        )
        result = fn().collect()
        assert result["n"].to_list() == [100.0]


# ---------------------------------------------------------------------------
# _build_output
# ---------------------------------------------------------------------------


class TestBuildOutput:
    """Tests for the output node builder."""

    def test_selects_specified_fields(self) -> None:
        _, fn, is_source = _build(
            "output",
            {"fields": ["x", "y"]},
            source_names=["upstream"],
        )
        assert is_source is False
        input_df = pl.DataFrame({"x": [1], "y": [2], "z": [3]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["x", "y"]

    def test_no_fields_returns_all(self) -> None:
        _, fn, _ = _build(
            "output",
            {"fields": []},
            source_names=["upstream"],
        )
        input_df = pl.DataFrame({"a": [1], "b": [2], "c": [3]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["a", "b", "c"]

    def test_none_fields_returns_all(self) -> None:
        _, fn, _ = _build(
            "output",
            {"fields": None},
            source_names=["upstream"],
        )
        input_df = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["a", "b"]

    def test_missing_fields_key_returns_all(self) -> None:
        _, fn, _ = _build("output", {}, source_names=["upstream"])
        input_df = pl.DataFrame({"a": [1]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["a"]

    def test_no_input_returns_empty(self) -> None:
        _, fn, _ = _build("output", {"fields": []})
        result = fn().collect()
        assert result.shape == (0, 0)

    def test_func_name_sanitized(self) -> None:
        func_name, _, _ = _build(
            "output",
            {"fields": ["x"]},
            label="Final Output (v2)",
            source_names=["upstream"],
        )
        # Should be a valid Python identifier
        assert func_name.isidentifier()


# ---------------------------------------------------------------------------
# _build_banding
# ---------------------------------------------------------------------------


class TestBuildBanding:
    """Tests for the banding node builder."""

    def test_continuous_single_factor(self) -> None:
        _, fn, is_source = _build(
            "banding",
            {
                "factors": [{
                    "column": "age",
                    "outputColumn": "age_band",
                    "banding": "continuous",
                    "rules": [
                        {"op1": ">=", "val1": 0, "op2": "<", "val2": 25, "assignment": "young"},
                        {"op1": ">=", "val1": 25, "op2": "<", "val2": 65, "assignment": "adult"},
                        {"op1": ">=", "val1": 65, "op2": "<=", "val2": 200, "assignment": "senior"},
                    ],
                }],
            },
            source_names=["upstream"],
        )
        assert is_source is False
        input_df = pl.DataFrame({"age": [10, 30, 70]}).lazy()
        result = fn(input_df).collect()
        assert "age_band" in result.columns
        assert result["age_band"].to_list() == ["young", "adult", "senior"]

    def test_categorical_banding(self) -> None:
        _, fn, _ = _build(
            "banding",
            {
                "factors": [{
                    "column": "vehicle",
                    "outputColumn": "vehicle_group",
                    "banding": "categorical",
                    "rules": [
                        {"value": "sedan", "assignment": "car"},
                        {"value": "suv", "assignment": "car"},
                        {"value": "motorcycle", "assignment": "bike"},
                    ],
                }],
            },
            source_names=["data"],
        )
        input_df = pl.DataFrame({"vehicle": ["sedan", "suv", "motorcycle"]}).lazy()
        result = fn(input_df).collect()
        assert result["vehicle_group"].to_list() == ["car", "car", "bike"]

    def test_multi_factor_banding(self) -> None:
        _, fn, _ = _build(
            "banding",
            {
                "factors": [
                    {
                        "column": "age",
                        "outputColumn": "age_band",
                        "banding": "continuous",
                        "rules": [
                            {
                                "op1": ">=", "val1": 0, "op2": "<",
                                "val2": 50, "assignment": "under50",
                            },
                            {
                                "op1": ">=", "val1": 50, "op2": "<=",
                                "val2": 200, "assignment": "50plus",
                            },
                        ],
                    },
                    {
                        "column": "region",
                        "outputColumn": "region_group",
                        "banding": "categorical",
                        "rules": [
                            {"value": "north", "assignment": "cold"},
                            {"value": "south", "assignment": "warm"},
                        ],
                    },
                ],
            },
            source_names=["data"],
        )
        input_df = pl.DataFrame({
            "age": [20, 60],
            "region": ["north", "south"],
        }).lazy()
        result = fn(input_df).collect()
        assert result["age_band"].to_list() == ["under50", "50plus"]
        assert result["region_group"].to_list() == ["cold", "warm"]

    def test_empty_factors_list_passthrough(self) -> None:
        _, fn, _ = _build("banding", {"factors": []}, source_names=["upstream"])
        input_df = pl.DataFrame({"x": [1, 2]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["x"]
        assert result["x"].to_list() == [1, 2]

    def test_no_factors_key_passthrough(self) -> None:
        _, fn, _ = _build("banding", {}, source_names=["upstream"])
        input_df = pl.DataFrame({"x": [1]}).lazy()
        result = fn(input_df).collect()
        assert result.columns == ["x"]

    def test_factor_with_missing_column_skipped(self) -> None:
        """A factor with empty column name should be skipped, not error."""
        _, fn, _ = _build(
            "banding",
            {
                "factors": [{
                    "column": "",
                    "outputColumn": "out",
                    "banding": "continuous",
                    "rules": [
                        {"op1": ">=", "val1": 0, "op2": "<", "val2": 10, "assignment": "low"},
                    ],
                }],
            },
            source_names=["data"],
        )
        input_df = pl.DataFrame({"x": [5]}).lazy()
        result = fn(input_df).collect()
        # Should pass through without adding column
        assert "out" not in result.columns

    def test_factor_with_empty_rules_skipped(self) -> None:
        _, fn, _ = _build(
            "banding",
            {
                "factors": [{
                    "column": "age",
                    "outputColumn": "age_band",
                    "banding": "continuous",
                    "rules": [],
                }],
            },
            source_names=["data"],
        )
        input_df = pl.DataFrame({"age": [25]}).lazy()
        result = fn(input_df).collect()
        assert "age_band" not in result.columns

    def test_continuous_with_default(self) -> None:
        _, fn, _ = _build(
            "banding",
            {
                "factors": [{
                    "column": "age",
                    "outputColumn": "age_band",
                    "banding": "continuous",
                    "default": "unknown",
                    "rules": [
                        {"op1": ">=", "val1": 0, "op2": "<", "val2": 25, "assignment": "young"},
                    ],
                }],
            },
            source_names=["data"],
        )
        input_df = pl.DataFrame({"age": [10, 50]}).lazy()
        result = fn(input_df).collect()
        bands = result["age_band"].to_list()
        assert bands[0] == "young"
        assert bands[1] == "unknown"


# ---------------------------------------------------------------------------
# _build_scenario_expander
# ---------------------------------------------------------------------------


class TestBuildScenarioExpander:
    """Tests for the scenario expander node builder.

    Note: basic coverage already exists in test_scenario_expander.py.
    These tests focus on edge cases and builder-specific behavior.
    """

    def test_is_not_a_source_node(self) -> None:
        _, _, is_source = _build(
            "scenarioExpander",
            {"column_name": "sv", "min_value": 0.9, "max_value": 1.1, "steps": 3},
            source_names=["upstream"],
        )
        assert is_source is False

    def test_empty_input_cross_join(self) -> None:
        """Expanding an empty input should yield 0 rows but correct columns."""
        _, fn, _ = _build(
            "scenarioExpander",
            {"column_name": "sv", "min_value": 0.9, "max_value": 1.1, "steps": 3},
            source_names=["upstream"],
        )
        input_df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int32)}).lazy()
        result = fn(input_df).collect()
        assert result.shape[0] == 0
        assert "sv" in result.columns

    def test_single_step(self) -> None:
        _, fn, _ = _build(
            "scenarioExpander",
            {"column_name": "val", "min_value": 1.0, "max_value": 1.0, "steps": 1},
            source_names=["upstream"],
        )
        input_df = pl.DataFrame({"x": [100]}).lazy()
        result = fn(input_df).collect()
        assert result.shape[0] == 1
        assert result["val"].to_list() == [pytest.approx(1.0)]

    def test_custom_step_column(self) -> None:
        _, fn, _ = _build(
            "scenarioExpander",
            {
                "column_name": "sv",
                "min_value": 0.5,
                "max_value": 1.5,
                "steps": 3,
                "step_column": "my_idx",
            },
            source_names=["upstream"],
        )
        input_df = pl.DataFrame({"x": [1]}).lazy()
        result = fn(input_df).collect()
        assert "my_idx" in result.columns
        assert result["my_idx"].to_list() == [0, 1, 2]


# ---------------------------------------------------------------------------
# _build_api_input
# ---------------------------------------------------------------------------


class TestBuildApiInput:
    """Tests for the API input node builder."""

    def test_parquet_source(self, tmp_path: Path, _widen_sandbox_root) -> None:
        data_file = tmp_path / "input.parquet"
        pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).write_parquet(data_file)

        func_name, fn, is_source = _build(
            "apiInput",
            {"path": str(data_file)},
            label="API_Data",
        )
        assert func_name == "API_Data"
        assert is_source is True
        result = fn().collect()
        assert result.shape == (3, 2)
        assert result["a"].to_list() == [1, 2, 3]

    def test_csv_source(self, tmp_path: Path, _widen_sandbox_root) -> None:
        data_file = tmp_path / "input.csv"
        pl.DataFrame({"x": [10, 20]}).write_csv(data_file)

        _, fn, is_source = _build(
            "apiInput",
            {"path": str(data_file)},
            label="CSV_Input",
        )
        assert is_source is True
        result = fn().collect()
        assert result.shape == (2, 1)

    def test_func_name_sanitized(self) -> None:
        func_name, _, _ = _build(
            "apiInput",
            {"path": "some.parquet"},
            label="My Input (v2)",
        )
        assert func_name.isidentifier()


# ---------------------------------------------------------------------------
# Builder dispatch fallback
# ---------------------------------------------------------------------------


class TestBuildNodeFnFallback:
    """Ensure unknown node types fall back to passthrough."""

    def test_unknown_type_passthrough(self) -> None:
        node = _n({
            "id": "n1",
            "data": {"label": "Unknown", "nodeType": "transform", "config": {}},
        })
        func_name, fn, is_source = _build_node_fn(node, source_names=["upstream"])
        assert func_name == "Unknown"
        assert is_source is False
        input_df = pl.DataFrame({"x": [1]}).lazy()
        result = fn(input_df).collect()
        assert result["x"].to_list() == [1]


# ---------------------------------------------------------------------------
# _build_data_sink (passthrough in preview mode)
# ---------------------------------------------------------------------------


class TestBuildDataSink:
    """Sink builder should be a passthrough during preview."""

    def test_passthrough(self) -> None:
        _, fn, is_source = _build(
            "dataSink",
            {"path": "out.parquet", "format": "parquet"},
            source_names=["upstream"],
        )
        assert is_source is False
        input_df = pl.DataFrame({"x": [1, 2]}).lazy()
        result = fn(input_df).collect()
        assert result["x"].to_list() == [1, 2]


# ---------------------------------------------------------------------------
# _build_optimiser (passthrough in preview mode)
# ---------------------------------------------------------------------------


class TestBuildOptimiser:
    """Optimiser builder should be a passthrough during preview."""

    def test_passthrough(self) -> None:
        _, fn, is_source = _build(
            "optimiser",
            {"mode": "online", "objective": "profit"},
            source_names=["upstream"],
        )
        assert is_source is False
        input_df = pl.DataFrame({"x": [1]}).lazy()
        result = fn(input_df).collect()
        assert result["x"].to_list() == [1]


# ---------------------------------------------------------------------------
# _build_live_switch
# ---------------------------------------------------------------------------


class TestBuildLiveSwitch:
    """Tests for the live switch node builder."""

    def test_selects_live_input(self) -> None:
        _, fn, _ = _build(
            "liveSwitch",
            {"input_scenario_map": {"live_src": "live", "batch_src": "test_batch"}},
            source_names=["live_src", "batch_src"],
        )
        live_df = pl.DataFrame({"source": ["live"]}).lazy()
        batch_df = pl.DataFrame({"source": ["batch"]}).lazy()
        result = fn(live_df, batch_df).collect()
        assert result["source"].to_list() == ["live"]

    def test_selects_non_live_scenario(self) -> None:
        node = _n({
            "id": "n1",
            "data": {
                "label": "Switch",
                "nodeType": "liveSwitch",
                "config": {"input_scenario_map": {"live_src": "live", "batch_src": "test_batch"}},
            },
        })
        _, fn, _ = _build_node_fn(
            node,
            source_names=["live_src", "batch_src"],
            scenario="test_batch",
        )
        live_df = pl.DataFrame({"source": ["live"]}).lazy()
        batch_df = pl.DataFrame({"source": ["batch"]}).lazy()
        result = fn(live_df, batch_df).collect()
        assert result["source"].to_list() == ["batch"]

    def test_fallback_to_first_input_on_unmapped_scenario(self) -> None:
        node = _n({
            "id": "n1",
            "data": {
                "label": "Switch",
                "nodeType": "liveSwitch",
                "config": {"input_scenario_map": {"a": "live", "b": "test"}},
            },
        })
        _, fn, _ = _build_node_fn(
            node,
            source_names=["a", "b"],
            scenario="unknown_scenario",
        )
        df_a = pl.DataFrame({"val": [1]}).lazy()
        df_b = pl.DataFrame({"val": [2]}).lazy()
        result = fn(df_a, df_b).collect()
        assert result["val"].to_list() == [1]
