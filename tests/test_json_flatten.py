"""Tests for haute._json_flatten — schema inference, flattening, and I/O."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from haute._json_flatten import (
    _infer_type,
    _wider_type,
    flatten,
    flatten_to_frame,
    infer_schema,
    load_samples,
    schema_columns,
)


# ---------------------------------------------------------------------------
# Type inference helpers
# ---------------------------------------------------------------------------


class TestInferType:
    def test_bool(self):
        assert _infer_type(True) == "bool"
        assert _infer_type(False) == "bool"

    def test_int(self):
        assert _infer_type(0) == "int"
        assert _infer_type(42) == "int"

    def test_float(self):
        assert _infer_type(3.14) == "float"
        assert _infer_type(0.0) == "float"

    def test_str(self):
        assert _infer_type("hello") == "str"
        assert _infer_type("") == "str"

    def test_bool_before_int(self):
        # bool is subclass of int — must detect bool first
        assert _infer_type(True) == "bool"


class TestWiderType:
    def test_same_types(self):
        assert _wider_type("int", "int") == "int"

    def test_int_float_widens_to_float(self):
        assert _wider_type("int", "float") == "float"
        assert _wider_type("float", "int") == "float"

    def test_anything_with_str_widens_to_str(self):
        assert _wider_type("int", "str") == "str"
        assert _wider_type("str", "bool") == "str"

    def test_bool_int_widens_to_int(self):
        assert _wider_type("bool", "int") == "int"


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


class TestInferSchema:
    def test_empty_samples(self):
        assert infer_schema([]) == {}

    def test_flat_dict(self):
        schema = infer_schema([{"name": "Alice", "age": 30}])
        assert schema == {"name": "str", "age": "int"}

    def test_nested_object(self):
        schema = infer_schema([{"address": {"city": "London", "postcode": "SW1"}}])
        assert schema == {"address": {"city": "str", "postcode": "str"}}

    def test_array_of_objects(self):
        schema = infer_schema([{
            "drivers": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ],
        }])
        assert schema == {
            "drivers": {
                "$max": 2,
                "$items": {"name": "str", "age": "int"},
            },
        }

    def test_array_of_scalars(self):
        schema = infer_schema([{"tags": ["a", "b", "c"]}])
        assert schema == {"tags": {"$max": 3, "$items": "str"}}

    def test_empty_array(self):
        schema = infer_schema([{"claims": []}])
        assert schema == {"claims": {"$max": 0, "$items": {}}}

    def test_null_value_defaults_to_str(self):
        schema = infer_schema([{"field": None}])
        assert schema == {"field": "str"}

    def test_null_then_object_resolves_to_object(self):
        schema = infer_schema([
            {"addr": None},
            {"addr": {"city": "Paris"}},
        ])
        assert schema == {"addr": {"city": "str"}}

    def test_multiple_samples_union_fields(self):
        schema = infer_schema([
            {"a": 1},
            {"a": 2, "b": "x"},
        ])
        assert schema == {"a": "int", "b": "str"}

    def test_multiple_samples_max_array_length(self):
        schema = infer_schema([
            {"items": [{"x": 1}]},
            {"items": [{"x": 2}, {"x": 3}, {"x": 4}]},
        ])
        assert schema["items"]["$max"] == 3

    def test_multiple_samples_union_array_fields(self):
        schema = infer_schema([
            {"items": [{"a": 1}]},
            {"items": [{"b": "x"}]},
        ])
        assert schema["items"]["$items"] == {"a": "int", "b": "str"}

    def test_type_widening_int_to_float(self):
        schema = infer_schema([
            {"value": 10},
            {"value": 3.14},
        ])
        assert schema == {"value": "float"}

    def test_deeply_nested(self):
        schema = infer_schema([{
            "a": {"b": {"c": {"d": 42}}},
        }])
        assert schema == {"a": {"b": {"c": {"d": "int"}}}}

    def test_nested_arrays(self):
        schema = infer_schema([{
            "drivers": [
                {"name": "Alice", "claims": [{"amount": 500}]},
            ],
        }])
        expected = {
            "drivers": {
                "$max": 1,
                "$items": {
                    "name": "str",
                    "claims": {
                        "$max": 1,
                        "$items": {"amount": "int"},
                    },
                },
            },
        }
        assert schema == expected


# ---------------------------------------------------------------------------
# Flattening
# ---------------------------------------------------------------------------


class TestFlatten:
    def test_flat_dict(self):
        schema = {"name": "str", "age": "int"}
        result = flatten({"name": "Alice", "age": 30}, schema)
        assert result == {"name": "Alice", "age": 30}

    def test_nested_object(self):
        schema = {"address": {"city": "str", "postcode": "str"}}
        result = flatten({"address": {"city": "London", "postcode": "SW1"}}, schema)
        assert result == {"address.city": "London", "address.postcode": "SW1"}

    def test_missing_data_gives_none(self):
        schema = {"a": "str", "b": "int"}
        result = flatten({"a": "hello"}, schema)
        assert result == {"a": "hello", "b": None}

    def test_null_data_gives_all_none(self):
        schema = {"a": "str", "b": {"c": "int"}}
        result = flatten(None, schema)
        assert result == {"a": None, "b.c": None}

    def test_array_one_based_indices(self):
        schema = {"items": {"$max": 2, "$items": {"x": "int"}}}
        result = flatten({"items": [{"x": 10}]}, schema)
        assert result == {"items.1.x": 10, "items.2.x": None}

    def test_array_of_scalars(self):
        schema = {"tags": {"$max": 3, "$items": "str"}}
        result = flatten({"tags": ["a", "b"]}, schema)
        assert result == {"tags.1": "a", "tags.2": "b", "tags.3": None}

    def test_empty_array_fills_nulls(self):
        schema = {"items": {"$max": 2, "$items": {"name": "str"}}}
        result = flatten({"items": []}, schema)
        assert result == {"items.1.name": None, "items.2.name": None}

    def test_array_truncates_to_max(self):
        schema = {"items": {"$max": 2, "$items": "int"}}
        result = flatten({"items": [1, 2, 3, 4]}, schema)
        assert result == {"items.1": 1, "items.2": 2}

    def test_max_zero_produces_no_columns(self):
        schema = {"items": {"$max": 0, "$items": {}}}
        result = flatten({"items": [1, 2, 3]}, schema)
        assert result == {}

    def test_empty_items_schema_produces_no_columns(self):
        schema = {"items": {"$max": 3, "$items": {}}}
        result = flatten({"items": [1, 2, 3]}, schema)
        assert result == {}

    def test_null_nested_object_gives_null_leaves(self):
        schema = {"addr": {"city": "str", "postcode": "str"}}
        result = flatten({"addr": None}, schema)
        assert result == {"addr.city": None, "addr.postcode": None}

    def test_deeply_nested(self):
        schema = {"a": {"b": {"c": "int"}}}
        result = flatten({"a": {"b": {"c": 42}}}, schema)
        assert result == {"a.b.c": 42}

    def test_nested_arrays(self):
        schema = {
            "drivers": {
                "$max": 2,
                "$items": {
                    "name": "str",
                    "claims": {"$max": 1, "$items": {"amount": "int"}},
                },
            },
        }
        data = {
            "drivers": [
                {"name": "Alice", "claims": [{"amount": 500}]},
            ],
        }
        result = flatten(data, schema)
        assert result == {
            "drivers.1.name": "Alice",
            "drivers.1.claims.1.amount": 500,
            "drivers.2.name": None,
            "drivers.2.claims.1.amount": None,
        }

    def test_extra_data_ignored(self):
        schema = {"a": "str"}
        result = flatten({"a": "hello", "b": "ignored"}, schema)
        assert result == {"a": "hello"}


# ---------------------------------------------------------------------------
# schema_columns
# ---------------------------------------------------------------------------


class TestSchemaColumns:
    def test_flat(self):
        schema = {"name": "str", "age": "int"}
        assert schema_columns(schema) == ["name", "age"]

    def test_nested(self):
        schema = {"a": {"b": "str", "c": "int"}}
        assert schema_columns(schema) == ["a.b", "a.c"]

    def test_array(self):
        schema = {"items": {"$max": 2, "$items": {"x": "int"}}}
        assert schema_columns(schema) == ["items.1.x", "items.2.x"]

    def test_max_zero_skipped(self):
        schema = {"items": {"$max": 0, "$items": {}}}
        assert schema_columns(schema) == []

    def test_matches_flatten_keys(self):
        schema = {
            "a": "str",
            "b": {"c": "int"},
            "d": {"$max": 2, "$items": {"e": "str"}},
        }
        cols = schema_columns(schema)
        flat = flatten(None, schema)
        assert cols == list(flat.keys())


# ---------------------------------------------------------------------------
# Consistency: same schema → same columns regardless of data
# ---------------------------------------------------------------------------


class TestConsistency:
    def test_different_data_same_columns(self):
        schema = {
            "name": "str",
            "items": {"$max": 2, "$items": {"x": "int"}},
        }
        flat_a = flatten({"name": "Alice", "items": [{"x": 1}]}, schema)
        flat_b = flatten({"name": "Bob"}, schema)
        flat_c = flatten(None, schema)
        assert list(flat_a.keys()) == list(flat_b.keys()) == list(flat_c.keys())

    def test_infer_then_flatten_round_trip(self):
        sample = {
            "id": 1,
            "meta": {"source": "web"},
            "tags": ["a", "b"],
        }
        schema = infer_schema([sample])
        flat = flatten(sample, schema)
        assert flat["id"] == 1
        assert flat["meta.source"] == "web"
        assert flat["tags.1"] == "a"
        assert flat["tags.2"] == "b"


# ---------------------------------------------------------------------------
# load_samples
# ---------------------------------------------------------------------------


class TestLoadSamples:
    def test_single_json_object(self, tmp_path: Path):
        p = tmp_path / "single.json"
        p.write_text(json.dumps({"a": 1}))
        result = load_samples(p)
        assert result == [{"a": 1}]

    def test_json_array(self, tmp_path: Path):
        p = tmp_path / "batch.json"
        p.write_text(json.dumps([{"a": 1}, {"a": 2}]))
        result = load_samples(p)
        assert len(result) == 2
        assert result[0] == {"a": 1}
        assert result[1] == {"a": 2}

    def test_jsonl(self, tmp_path: Path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a": 1}\n{"a": 2}\n{"a": 3}\n')
        result = load_samples(p)
        assert len(result) == 3

    def test_jsonl_blank_lines_skipped(self, tmp_path: Path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a": 1}\n\n\n{"a": 2}\n')
        result = load_samples(p)
        assert len(result) == 2

    def test_json_non_dict_elements_filtered(self, tmp_path: Path):
        p = tmp_path / "mixed.json"
        p.write_text(json.dumps([{"a": 1}, 42, "string", {"b": 2}]))
        result = load_samples(p)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# flatten_to_frame
# ---------------------------------------------------------------------------


class TestFlattenToFrame:
    def test_single_dict(self):
        schema = {"name": "str", "age": "int"}
        lf = flatten_to_frame({"name": "Alice", "age": 30}, schema)
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert df.shape == (1, 2)
        assert df["name"][0] == "Alice"
        assert df["age"][0] == 30

    def test_multiple_dicts(self):
        schema = {"x": "int"}
        lf = flatten_to_frame([{"x": 1}, {"x": 2}, {"x": 3}], schema)
        df = lf.collect()
        assert df.shape == (3, 1)
        assert df["x"].to_list() == [1, 2, 3]

    def test_missing_values_are_null(self):
        schema = {"a": "str", "b": "int"}
        lf = flatten_to_frame([{"a": "x"}, {"b": 5}], schema)
        df = lf.collect()
        assert df.shape == (2, 2)
        assert df["b"][0] is None
        assert df["a"][1] is None

    def test_consistent_columns_across_rows(self):
        schema = {
            "name": "str",
            "items": {"$max": 2, "$items": {"val": "int"}},
        }
        lf = flatten_to_frame(
            [
                {"name": "A", "items": [{"val": 1}, {"val": 2}]},
                {"name": "B", "items": []},
            ],
            schema,
        )
        df = lf.collect()
        assert df.columns == ["name", "items.1.val", "items.2.val"]
        assert df.shape == (2, 3)


# ---------------------------------------------------------------------------
# Real data: sample_quote.json
# ---------------------------------------------------------------------------


SAMPLE_QUOTE_PATH = Path(__file__).parent.parent / "data" / "sample_quote.json"


@pytest.mark.skipif(
    not SAMPLE_QUOTE_PATH.exists(),
    reason="data/sample_quote.json not found",
)
class TestSampleQuote:
    @pytest.fixture()
    def quote(self) -> dict:
        return json.loads(SAMPLE_QUOTE_PATH.read_text())

    @pytest.fixture()
    def schema(self, quote: dict) -> dict:
        return infer_schema([quote])

    def test_infer_produces_non_empty_schema(self, schema: dict):
        assert len(schema) > 0

    def test_top_level_keys(self, schema: dict):
        expected = {
            "quote_metadata", "policy_details", "proposer",
            "additional_drivers", "vehicle", "address",
            "previous_address", "add_ons",
        }
        assert expected == set(schema.keys())

    def test_additional_drivers_is_array(self, schema: dict):
        ad = schema["additional_drivers"]
        assert "$max" in ad
        assert ad["$max"] == 1  # sample has 1 additional driver

    def test_empty_arrays_detected(self, schema: dict):
        # proposer.claims is [] in the sample
        claims = schema["proposer"]["claims"]
        assert claims == {"$max": 0, "$items": {}}

    def test_flatten_produces_consistent_columns(self, quote: dict, schema: dict):
        flat = flatten(quote, schema)
        cols = schema_columns(schema)
        assert list(flat.keys()) == cols

    def test_flatten_preserves_values(self, quote: dict, schema: dict):
        flat = flatten(quote, schema)
        assert flat["quote_metadata.quote_id"] == "QUO-2026-000000001"
        assert flat["proposer.date_of_birth"] == "1990-11-17"
        assert flat["vehicle.make"] == "Ford"
        assert flat["address.postcode"] == "PH16 5LD"
        assert flat["policy_details.ncd_protected"] is False
        assert flat["additional_drivers.1.first_name"] == "Aurora"
        assert flat["add_ons.breakdown_cover.selected"] is True
        assert flat["add_ons.breakdown_cover.level"] == "european"

    def test_null_previous_address(self, quote: dict, schema: dict):
        flat = flatten(quote, schema)
        # previous_address is null → inferred as "str" → single null column
        assert flat["previous_address"] is None

    def test_flatten_to_frame_shape(self, quote: dict, schema: dict):
        lf = flatten_to_frame(quote, schema)
        df = lf.collect()
        assert df.shape[0] == 1
        assert df.shape[1] == len(schema_columns(schema))

    def test_two_quotes_same_columns(self, quote: dict, schema: dict):
        # Flatten the same quote twice — columns must match
        lf = flatten_to_frame([quote, quote], schema)
        df = lf.collect()
        assert df.shape == (2, len(schema_columns(schema)))
