"""Tests for haute._rating — rating table lookup and banding helpers."""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from haute._rating import (
    _apply_banding,
    _apply_rating_table,
    _banding_condition,
    _combine_rating_columns,
    _normalise_banding_factors,
)


# ---------------------------------------------------------------------------
# _banding_condition
# ---------------------------------------------------------------------------


class TestBandingCondition:
    def test_single_operator(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": "<", "val1": 10})
        assert cond is not None
        lf = pl.DataFrame({"x": [5, 10, 15]}).lazy()
        result = lf.select(cond).collect()["x"].to_list()
        assert result == [True, False, False]

    def test_dual_operator_range(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": ">=", "val1": 5, "op2": "<", "val2": 10})
        assert cond is not None
        lf = pl.DataFrame({"x": [3, 5, 7, 10]}).lazy()
        result = lf.select(cond).collect()["x"].to_list()
        assert result == [False, True, True, False]

    def test_eq_operator(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": "=", "val1": 5})
        assert cond is not None
        lf = pl.DataFrame({"x": [4, 5, 6]}).lazy()
        result = lf.select(cond).collect()["x"].to_list()
        assert result == [False, True, False]

    def test_double_eq_operator(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": "==", "val1": 5})
        assert cond is not None
        lf = pl.DataFrame({"x": [4, 5]}).lazy()
        result = lf.select(cond).collect()["x"].to_list()
        assert result == [False, True]

    def test_empty_rule_returns_none(self) -> None:
        assert _banding_condition(pl.col("x"), {}) is None

    def test_missing_val_returns_none(self) -> None:
        assert _banding_condition(pl.col("x"), {"op1": "<", "val1": ""}) is None

    def test_none_val_returns_none(self) -> None:
        assert _banding_condition(pl.col("x"), {"op1": "<", "val1": None}) is None

    def test_invalid_op_ignored(self) -> None:
        assert _banding_condition(pl.col("x"), {"op1": "!=", "val1": 5}) is None

    def test_string_val_coerced_to_float(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": ">", "val1": "100"})
        assert cond is not None

    def test_non_numeric_val_ignored(self) -> None:
        assert _banding_condition(pl.col("x"), {"op1": ">", "val1": "abc"}) is None

    def test_whitespace_in_op(self) -> None:
        cond = _banding_condition(pl.col("x"), {"op1": " <= ", "val1": 10})
        assert cond is not None


# ---------------------------------------------------------------------------
# _normalise_banding_factors
# ---------------------------------------------------------------------------


class TestNormaliseBandingFactors:
    def test_returns_factors_list(self) -> None:
        config = {"factors": [{"column": "a"}]}
        assert _normalise_banding_factors(config) == [{"column": "a"}]

    def test_missing_factors_returns_empty(self) -> None:
        assert _normalise_banding_factors({}) == []

    def test_none_factors_returns_empty(self) -> None:
        assert _normalise_banding_factors({"factors": None}) == []

    def test_non_list_returns_empty(self) -> None:
        assert _normalise_banding_factors({"factors": "invalid"}) == []


# ---------------------------------------------------------------------------
# _apply_rating_table
# ---------------------------------------------------------------------------


class TestApplyRatingTable:
    def test_single_factor_lookup(self) -> None:
        lf = pl.DataFrame({"region": ["North", "South", "East"]}).lazy()
        table: dict[str, Any] = {
            "factors": ["region"],
            "outputColumn": "region_factor",
            "entries": [
                {"region": "North", "value": 1.2},
                {"region": "South", "value": 0.9},
            ],
        }
        result = _apply_rating_table(lf, table).collect()
        assert result["region_factor"].to_list() == [1.2, 0.9, None]

    def test_multi_factor_lookup(self) -> None:
        lf = pl.DataFrame({
            "region": ["North", "North", "South"],
            "tier": ["gold", "silver", "gold"],
        }).lazy()
        table: dict[str, Any] = {
            "factors": ["region", "tier"],
            "outputColumn": "rate",
            "entries": [
                {"region": "North", "tier": "gold", "value": 1.5},
                {"region": "North", "tier": "silver", "value": 1.0},
                {"region": "South", "tier": "gold", "value": 0.8},
            ],
        }
        result = _apply_rating_table(lf, table).collect()
        assert result["rate"].to_list() == [1.5, 1.0, 0.8]

    def test_default_value(self) -> None:
        lf = pl.DataFrame({"region": ["North", "Unknown"]}).lazy()
        table: dict[str, Any] = {
            "factors": ["region"],
            "outputColumn": "factor",
            "entries": [{"region": "North", "value": 1.2}],
            "defaultValue": "1.0",
        }
        result = _apply_rating_table(lf, table).collect()
        assert result["factor"].to_list() == [1.2, 1.0]

    def test_no_default_leaves_null(self) -> None:
        lf = pl.DataFrame({"region": ["North", "Missing"]}).lazy()
        table: dict[str, Any] = {
            "factors": ["region"],
            "outputColumn": "factor",
            "entries": [{"region": "North", "value": 1.5}],
        }
        result = _apply_rating_table(lf, table).collect()
        assert result["factor"][1] is None

    def test_empty_factors_passthrough(self) -> None:
        lf = pl.DataFrame({"x": [1]}).lazy()
        table: dict[str, Any] = {"factors": [], "outputColumn": "y", "entries": []}
        result = _apply_rating_table(lf, table).collect()
        assert "y" not in result.columns

    def test_empty_entries_passthrough(self) -> None:
        lf = pl.DataFrame({"x": [1]}).lazy()
        table: dict[str, Any] = {"factors": ["x"], "outputColumn": "y", "entries": []}
        result = _apply_rating_table(lf, table).collect()
        assert "y" not in result.columns

    def test_missing_output_column_passthrough(self) -> None:
        lf = pl.DataFrame({"x": [1]}).lazy()
        table: dict[str, Any] = {"factors": ["x"], "outputColumn": "", "entries": [{"x": 1, "value": 2}]}
        result = _apply_rating_table(lf, table).collect()
        assert result.columns == ["x"]

    def test_numeric_factor_cast_to_string(self) -> None:
        """Integer factor columns should be cast to Utf8 for the join."""
        lf = pl.DataFrame({"code": [1, 2, 3]}).lazy()
        table: dict[str, Any] = {
            "factors": ["code"],
            "outputColumn": "val",
            "entries": [
                {"code": 1, "value": 10.0},
                {"code": 2, "value": 20.0},
            ],
        }
        result = _apply_rating_table(lf, table).collect()
        assert result["val"].to_list() == [10.0, 20.0, None]

    def test_output_column_named_value(self) -> None:
        """When outputColumn is 'value', the temporary column should not be dropped."""
        lf = pl.DataFrame({"k": ["a"]}).lazy()
        table: dict[str, Any] = {
            "factors": ["k"],
            "outputColumn": "value",
            "entries": [{"k": "a", "value": 5.0}],
        }
        result = _apply_rating_table(lf, table).collect()
        assert "value" in result.columns
        assert result["value"].to_list() == [5.0]


# ---------------------------------------------------------------------------
# _combine_rating_columns
# ---------------------------------------------------------------------------


class TestCombineRatingColumns:
    def test_multiply_default(self) -> None:
        lf = pl.DataFrame({"a": [2.0], "b": [3.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b"], "multiply", "combined").collect()
        assert result["combined"].to_list() == [6.0]

    def test_add(self) -> None:
        lf = pl.DataFrame({"a": [10.0], "b": [5.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b"], "add", "sum_col").collect()
        assert result["sum_col"].to_list() == [15.0]

    def test_min(self) -> None:
        lf = pl.DataFrame({"a": [3.0], "b": [7.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b"], "min", "min_col").collect()
        assert result["min_col"].to_list() == [3.0]

    def test_max(self) -> None:
        lf = pl.DataFrame({"a": [3.0], "b": [7.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b"], "max", "max_col").collect()
        assert result["max_col"].to_list() == [7.0]

    def test_single_column_aliased(self) -> None:
        lf = pl.DataFrame({"a": [42.0]}).lazy()
        result = _combine_rating_columns(lf, ["a"], "multiply", "out").collect()
        assert result["out"].to_list() == [42.0]

    def test_empty_columns_passthrough(self) -> None:
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _combine_rating_columns(lf, [], "multiply", "out").collect()
        assert "out" not in result.columns

    def test_three_columns_multiply(self) -> None:
        lf = pl.DataFrame({"a": [2.0], "b": [3.0], "c": [4.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b", "c"], "multiply", "prod").collect()
        assert result["prod"].to_list() == [24.0]

    def test_three_columns_add(self) -> None:
        lf = pl.DataFrame({"a": [1.0], "b": [2.0], "c": [3.0]}).lazy()
        result = _combine_rating_columns(lf, ["a", "b", "c"], "add", "total").collect()
        assert result["total"].to_list() == [6.0]
