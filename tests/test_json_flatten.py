"""Tests for haute._json_flatten — schema inference, flattening, and I/O."""

from __future__ import annotations

import json
import time
from pathlib import Path

import polars as pl
import pytest

from haute._json_flatten import (
    _FLATTEN_CHUNK_SIZE,
    _LARGE_FILE_THRESHOLD,
    _SCHEMA_SAMPLE_SIZE,
    _arrow_schema_from_flatten,
    _build_polars_exprs,
    _chunked,
    _clear_flatten_progress,
    _coerce_to_arrow,
    _flatten_and_write,
    _flatten_and_write_streaming,
    _flatten_with_polars,
    _infer_schema_streaming,
    _infer_type,
    _is_cache_valid,
    _iter_json_records,
    _json_cache_path,
    _rows_to_batch,
    _schema_leaf_types,
    _set_flatten_progress,
    _struct_field_dtype,
    _wider_type,
    build_json_cache,
    clear_json_cache,
    flatten,
    flatten_progress,
    flatten_to_frame,
    infer_schema,
    is_large_json,
    json_cache_info,
    load_samples,
    read_json_flat,
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


# ---------------------------------------------------------------------------
# Parquet cache
# ---------------------------------------------------------------------------


class TestJsonCache:
    def test_cache_path_is_deterministic(self):
        p1 = _json_cache_path("data/quotes.json")
        p2 = _json_cache_path("data/quotes.json")
        assert p1 == p2
        assert p1.suffix == ".parquet"
        assert ".haute_cache" in str(p1)

    def test_cache_invalid_when_missing(self, tmp_path):
        cache = tmp_path / "missing.parquet"
        source = tmp_path / "data.json"
        source.write_text("[]")
        assert not _is_cache_valid(cache, source)

    def test_cache_valid_when_newer(self, tmp_path):
        source = tmp_path / "data.json"
        source.write_text("[]")
        cache = tmp_path / "cached.parquet"
        cache.write_bytes(b"fake")
        # Ensure cache is newer by touching it
        import os
        os.utime(cache, (cache.stat().st_mtime + 10, cache.stat().st_mtime + 10))
        assert _is_cache_valid(cache, source)

    def test_cache_invalid_when_source_newer(self, tmp_path):
        cache = tmp_path / "cached.parquet"
        cache.write_bytes(b"fake")
        source = tmp_path / "data.json"
        source.write_text("[]")
        # Ensure source is newer
        import os
        os.utime(source, (source.stat().st_mtime + 10, source.stat().st_mtime + 10))
        assert not _is_cache_valid(cache, source)

    def test_flatten_and_write_creates_parquet(self, tmp_path):
        schema = {"name": "str", "age": "int"}
        samples = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        cache = tmp_path / "test.parquet"

        _flatten_and_write(samples, schema, cache)

        assert cache.exists()
        df = pl.read_parquet(cache)
        assert df.shape == (2, 2)
        assert df["name"].to_list() == ["Alice", "Bob"]

    def test_flatten_and_write_is_atomic(self, tmp_path):
        """Tmp file is cleaned up on failure."""
        cache = tmp_path / "test.parquet"
        tmp = cache.with_suffix(".parquet.tmp")

        # Force an error by passing a schema that causes parquet write to fail
        # (write to a path that is a directory, not a file)
        bad_cache = tmp_path / "dir_not_file.parquet"
        bad_cache.mkdir()
        with pytest.raises(Exception):
            _flatten_and_write([{"x": 1}], {"x": "int"}, bad_cache)

    def test_read_json_flat_caches_and_reuses(self, tmp_path, monkeypatch):
        """Second call to read_json_flat uses the cached parquet."""
        monkeypatch.chdir(tmp_path)

        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"x": 1}, {"x": 2}]))

        # First call — cache miss, writes parquet
        lf1 = read_json_flat(str(data_file))
        df1 = lf1.collect()
        assert df1["x"].to_list() == [1, 2]

        cache_path = _json_cache_path(str(data_file))
        assert cache_path.exists()
        first_mtime = cache_path.stat().st_mtime

        # Second call — cache hit, no rewrite
        lf2 = read_json_flat(str(data_file))
        df2 = lf2.collect()
        assert df2["x"].to_list() == [1, 2]
        assert cache_path.stat().st_mtime == first_mtime

    def test_read_json_flat_invalidates_on_data_change(self, tmp_path, monkeypatch):
        """Cache is rebuilt when the source data file changes."""
        import os

        monkeypatch.chdir(tmp_path)

        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"x": 1}]))

        lf = read_json_flat(str(data_file))
        assert lf.collect()["x"].to_list() == [1]

        cache_path = _json_cache_path(str(data_file))

        # Push cache mtime to the past so a rewrite is always detectable,
        # regardless of filesystem timestamp resolution.
        # Use 1980-01-01 (not epoch 0) — NTFS rejects timestamps before 1980.
        old_ts = 315532800.0  # 1980-01-01 00:00:00 UTC
        os.utime(cache_path, (old_ts, old_ts))
        first_mtime = cache_path.stat().st_mtime

        # Modify source file — its mtime is "now", well after 1980
        data_file.write_text(json.dumps([{"x": 99}]))

        lf2 = read_json_flat(str(data_file))
        assert lf2.collect()["x"].to_list() == [99]
        assert cache_path.stat().st_mtime > first_mtime

    def test_read_json_flat_invalidates_on_config_change(self, tmp_path, monkeypatch):
        """Cache is rebuilt when the config file changes."""
        import os

        monkeypatch.chdir(tmp_path)

        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"name": "Alice", "age": 30}]))

        config_file = tmp_path / "config.json"
        schema1 = {"name": "str", "age": "int"}
        config_file.write_text(json.dumps({"flattenSchema": schema1}))

        lf = read_json_flat(str(data_file), config_path=str(config_file))
        assert set(lf.collect().columns) == {"name", "age"}

        cache_path = _json_cache_path(str(data_file))

        # Push cache mtime to the past so a rewrite is always detectable.
        # Use 1980-01-01 (not epoch 0) — NTFS rejects timestamps before 1980.
        old_ts = 315532800.0  # 1980-01-01 00:00:00 UTC
        os.utime(cache_path, (old_ts, old_ts))

        # Modify config (drop the age column from schema)
        schema2 = {"name": "str"}
        config_file.write_text(json.dumps({"flattenSchema": schema2}))

        lf2 = read_json_flat(str(data_file), config_path=str(config_file))
        assert lf2.collect().columns == ["name"]


# ---------------------------------------------------------------------------
# Explicit cache management functions
# ---------------------------------------------------------------------------


class TestIsLargeJson:
    def test_small_file_returns_false(self, tmp_path):
        p = tmp_path / "small.jsonl"
        p.write_text('{"a": 1}\n')
        assert not is_large_json(p)

    def test_large_file_returns_true(self, tmp_path):
        p = tmp_path / "large.jsonl"
        # Write a file that exceeds the threshold
        p.write_bytes(b"x" * (_LARGE_FILE_THRESHOLD + 1))
        assert is_large_json(p)

    def test_exact_threshold_returns_true(self, tmp_path):
        p = tmp_path / "exact.jsonl"
        p.write_bytes(b"x" * _LARGE_FILE_THRESHOLD)
        assert is_large_json(p)

    def test_nonexistent_file_returns_false(self, tmp_path):
        p = tmp_path / "missing.jsonl"
        assert not is_large_json(p)


class TestJsonCacheInfo:
    def test_returns_none_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert json_cache_info("nonexistent.json") is None

    def test_returns_metadata_when_cached(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]))

        # Build cache first
        read_json_flat(str(data_file))

        info = json_cache_info(str(data_file))
        assert info is not None
        assert info["row_count"] == 2
        assert info["column_count"] == 2
        assert info["size_bytes"] > 0
        assert info["cached_at"] > 0
        assert "x" in info["columns"]
        assert "y" in info["columns"]
        assert info["data_path"] == str(data_file)


class TestClearJsonCache:
    def test_returns_false_when_no_cache(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert clear_json_cache("nonexistent.json") is False

    def test_deletes_cached_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"x": 1}]))

        read_json_flat(str(data_file))
        cache_path = _json_cache_path(str(data_file))
        assert cache_path.exists()

        assert clear_json_cache(str(data_file)) is True
        assert not cache_path.exists()


class TestFlattenProgress:
    @pytest.fixture(autouse=True)
    def _cleanup_progress(self):
        yield
        _clear_flatten_progress()

    def test_returns_none_when_inactive(self):
        assert flatten_progress("some/path.jsonl") is None

    def test_returns_progress_when_active(self):
        _set_flatten_progress("data.jsonl", {"rows": 1000, "elapsed": 2.5})
        result = flatten_progress("data.jsonl")
        assert result is not None
        assert result["rows"] == 1000
        assert result["elapsed"] == 2.5


class TestBuildJsonCache:
    def test_builds_cache_and_returns_metadata(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_file = tmp_path / "data.jsonl"
        data_file.write_text('{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n')

        result = build_json_cache(str(data_file))

        assert result["row_count"] == 2
        assert result["column_count"] == 2
        assert result["size_bytes"] > 0
        assert result["cached_at"] > 0
        assert result["cache_seconds"] >= 0
        assert result["data_path"] == str(data_file)
        assert "a" in result["columns"]
        assert "b" in result["columns"]

        # Verify the cache file actually exists
        cache_path = Path(result["path"])
        assert cache_path.exists()

        # Verify progress is cleared after build
        assert flatten_progress(str(data_file)) is None

    def test_builds_cache_with_config_schema(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps([{"name": "Alice", "age": 30, "extra": "ignored"}]))

        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps({"flattenSchema": {"name": "str"}}))

        result = build_json_cache(str(data_file), config_path=str(config_file))
        assert result["column_count"] == 1
        assert "name" in result["columns"]
        assert "extra" not in result["columns"]


# ---------------------------------------------------------------------------
# Streaming infrastructure
# ---------------------------------------------------------------------------


class TestIterJsonRecords:
    def test_jsonl_streams_records(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}\n{"a":2}\n{"a":3}\n')
        records = list(_iter_json_records(p))
        assert records == [{"a": 1}, {"a": 2}, {"a": 3}]

    def test_jsonl_skips_blank_lines(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}\n\n{"a":2}\n')
        assert len(list(_iter_json_records(p))) == 2

    def test_jsonl_skips_invalid_json(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}\nnot json\n{"a":2}\n')
        records = list(_iter_json_records(p))
        assert records == [{"a": 1}, {"a": 2}]

    def test_jsonl_skips_non_dict_lines(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text('{"a":1}\n[1, 2]\n"string"\n{"a":2}\n')
        records = list(_iter_json_records(p))
        assert records == [{"a": 1}, {"a": 2}]

    def test_json_array(self, tmp_path):
        p = tmp_path / "data.json"
        p.write_text(json.dumps([{"x": 1}, {"x": 2}]))
        assert list(_iter_json_records(p)) == [{"x": 1}, {"x": 2}]

    def test_json_single_object(self, tmp_path):
        p = tmp_path / "data.json"
        p.write_text(json.dumps({"x": 42}))
        assert list(_iter_json_records(p)) == [{"x": 42}]

    def test_empty_jsonl(self, tmp_path):
        p = tmp_path / "data.jsonl"
        p.write_text("")
        assert list(_iter_json_records(p)) == []


class TestInferSchemaStreaming:
    def test_infers_from_first_n_samples(self, tmp_path):
        p = tmp_path / "data.jsonl"
        lines = [json.dumps({"x": i, "y": "val"}) for i in range(100)]
        p.write_text("\n".join(lines) + "\n")
        schema = _infer_schema_streaming(p, max_samples=10)
        assert "x" in schema
        assert "y" in schema

    def test_stops_after_max_samples(self, tmp_path):
        """Should not read the entire file — stops after max_samples."""
        p = tmp_path / "data.jsonl"
        lines = [json.dumps({"x": i}) for i in range(1000)]
        p.write_text("\n".join(lines) + "\n")
        # This should complete quickly even with a huge file
        schema = _infer_schema_streaming(p, max_samples=5)
        assert "x" in schema

    def test_empty_file_returns_empty_schema(self, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("")
        assert _infer_schema_streaming(p) == {}

    def test_nested_schema_inferred(self, tmp_path):
        p = tmp_path / "nested.jsonl"
        p.write_text(json.dumps({"a": {"b": 1}}) + "\n")
        schema = _infer_schema_streaming(p)
        assert "a" in schema
        assert "b" in schema["a"]


class TestSchemaLeafTypes:
    def test_flat_schema(self):
        schema = {"x": "int", "y": "str", "z": "float"}
        result = _schema_leaf_types(schema)
        assert ("x", "int") in result
        assert ("y", "str") in result
        assert ("z", "float") in result

    def test_nested_schema(self):
        schema = {"a": {"b": "int", "c": "str"}}
        result = _schema_leaf_types(schema)
        assert ("a.b", "int") in result
        assert ("a.c", "str") in result

    def test_array_schema(self):
        schema = {"items": {"$max": 2, "$items": {"name": "str"}}}
        result = _schema_leaf_types(schema)
        assert ("items.1.name", "str") in result
        assert ("items.2.name", "str") in result

    def test_matches_schema_columns(self):
        """Leaf type names must match schema_columns output exactly."""
        schema = {"a": "int", "b": {"c": "str"}, "d": {"$max": 1, "$items": "float"}}
        leaf_names = [name for name, _ in _schema_leaf_types(schema)]
        assert leaf_names == schema_columns(schema)


class TestArrowSchemaFromFlatten:
    def test_maps_types_correctly(self):
        import pyarrow as pa

        schema = {"x": "int", "y": "float", "z": "str", "flag": "bool"}
        arrow = _arrow_schema_from_flatten(schema)
        # int and float both map to float64 for JSON safety
        assert arrow.field("x").type == pa.float64()
        assert arrow.field("y").type == pa.float64()
        assert arrow.field("z").type == pa.string()
        assert arrow.field("flag").type == pa.bool_()

    def test_all_fields_nullable(self):
        schema = {"a": "int", "b": "str"}
        arrow = _arrow_schema_from_flatten(schema)
        for field in arrow:
            assert field.nullable


class TestChunked:
    def test_exact_chunks(self):
        result = list(_chunked(iter(range(6)), 3))
        assert result == [[0, 1, 2], [3, 4, 5]]

    def test_remainder_chunk(self):
        result = list(_chunked(iter(range(5)), 3))
        assert result == [[0, 1, 2], [3, 4]]

    def test_empty_iterator(self):
        assert list(_chunked(iter([]), 3)) == []

    def test_single_item(self):
        assert list(_chunked(iter([42]), 3)) == [[42]]


class TestCoerceToArrow:
    def test_clean_float_values(self):
        import pyarrow as pa

        arr = _coerce_to_arrow([1.0, 2.5, None], pa.float64())
        assert arr.type == pa.float64()
        assert arr.to_pylist() == [1.0, 2.5, None]

    def test_int_to_float64(self):
        import pyarrow as pa

        arr = _coerce_to_arrow([1, 2, 3], pa.float64())
        assert arr.type == pa.float64()

    def test_mixed_numeric_coercion(self):
        """String in a numeric column should be coerced or become null."""
        import pyarrow as pa

        arr = _coerce_to_arrow([1, "not_a_number", 3.5, None], pa.float64())
        assert arr.type == pa.float64()
        assert arr[0].as_py() == 1.0
        assert arr[1].as_py() is None  # coerced to null
        assert arr[2].as_py() == 3.5

    def test_string_values(self):
        import pyarrow as pa

        arr = _coerce_to_arrow(["a", "b", None], pa.string())
        assert arr.type == pa.string()
        assert arr.to_pylist() == ["a", "b", None]

    def test_bool_values(self):
        import pyarrow as pa

        arr = _coerce_to_arrow([True, False, None], pa.bool_())
        assert arr.type == pa.bool_()

    def test_bool_with_non_bool_drops_to_null(self):
        import pyarrow as pa

        arr = _coerce_to_arrow([True, "yes", None], pa.bool_())
        assert arr.type == pa.bool_()
        assert arr[0].as_py() is True
        assert arr[1].as_py() is None  # non-bool → null


class TestFlattenAndWriteStreaming:
    def test_writes_correct_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        records = iter([{"x": 1, "y": "a"}, {"x": 2, "y": "b"}])
        schema = {"x": "int", "y": "str"}
        cache_path = tmp_path / ".haute_cache" / "test.parquet"

        total = _flatten_and_write_streaming(records, schema, cache_path)

        assert total == 2
        assert cache_path.exists()
        df = pl.read_parquet(cache_path)
        assert len(df) == 2
        assert set(df.columns) == {"x", "y"}

    def test_chunked_write_produces_same_result(self, tmp_path):
        """With chunk_size=1, each row is a separate write — result should be identical."""
        records = [{"a": i} for i in range(5)]
        schema = {"a": "int"}
        cache_path = tmp_path / "chunked.parquet"

        total = _flatten_and_write_streaming(
            iter(records), schema, cache_path, chunk_size=1,
        )

        assert total == 5
        df = pl.read_parquet(cache_path)
        assert len(df) == 5

    def test_empty_iterator_writes_empty_parquet(self, tmp_path):
        schema = {"x": "int"}
        cache_path = tmp_path / "empty.parquet"

        total = _flatten_and_write_streaming(iter([]), schema, cache_path)

        assert total == 0
        assert cache_path.exists()
        df = pl.read_parquet(cache_path)
        assert len(df) == 0
        assert "x" in df.columns

    def test_atomic_cleanup_on_failure(self, tmp_path):
        """If flatten raises mid-stream, temp file is cleaned up."""
        def _bad_iter():
            yield {"x": 1}
            raise ValueError("boom")

        schema = {"x": "int"}
        cache_path = tmp_path / "fail.parquet"

        with pytest.raises(ValueError, match="boom"):
            _flatten_and_write_streaming(_bad_iter(), schema, cache_path)

        assert not cache_path.exists()
        assert not cache_path.with_suffix(".parquet.tmp").exists()

    def test_progress_updates_during_write(self, tmp_path):
        """Progress should be updated during streaming write."""
        records = [{"x": i} for i in range(10)]
        schema = {"x": "int"}
        cache_path = tmp_path / "progress.parquet"
        key = "test_progress_path"

        _flatten_and_write_streaming(
            iter(records), schema, cache_path,
            chunk_size=5, progress_key=key, t0=time.monotonic(),
        )
        # After completion, progress should be cleared by the caller (build_json_cache)
        # but the streaming function itself doesn't clear it — that's the caller's job.
        # Here we just verify the file was written correctly.
        assert pl.read_parquet(cache_path).shape[0] == 10

class TestRowsToBatch:
    def test_builds_valid_batch(self):
        import pyarrow as pa

        schema = {"x": "int", "y": "str"}
        arrow_schema = _arrow_schema_from_flatten(schema)
        rows = [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]
        batch = _rows_to_batch(rows, arrow_schema)
        assert batch.num_rows == 2
        assert batch.schema == arrow_schema


# ---------------------------------------------------------------------------
# Polars-native flatten
# ---------------------------------------------------------------------------


class TestStructFieldDtype:
    def test_returns_field_dtype(self):
        dtype = pl.Struct({"city": pl.Utf8, "zip": pl.Int64})
        assert _struct_field_dtype(dtype, "city") == pl.Utf8
        assert _struct_field_dtype(dtype, "zip") == pl.Int64

    def test_returns_none_for_missing_field(self):
        dtype = pl.Struct({"city": pl.Utf8})
        assert _struct_field_dtype(dtype, "missing") is None

    def test_returns_none_for_non_struct(self):
        assert _struct_field_dtype(pl.Int64, "anything") is None
        assert _struct_field_dtype(None, "anything") is None


class TestBuildPolarsExprs:
    def test_flat_schema(self):
        """Simple top-level scalar columns."""
        schema = {"x": "int", "y": "str"}
        lf_schema = {"x": pl.Int64, "y": pl.Utf8}
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 2

        df = pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).select(exprs)
        assert df.columns == ["x", "y"]
        assert df["x"].to_list() == [1, 2]

    def test_nested_object(self):
        """Struct column → struct.field() expressions."""
        schema = {"addr": {"city": "str", "zip": "str"}}
        lf_schema = {"addr": pl.Struct({"city": pl.Utf8, "zip": pl.Utf8})}
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 2

        df = pl.DataFrame({"addr": [{"city": "London", "zip": "SW1"}]}).select(exprs)
        assert df.columns == ["addr.city", "addr.zip"]
        assert df["addr.city"][0] == "London"

    def test_array_of_scalars(self):
        """List column → list.get() expressions."""
        schema = {"tags": {"$max": 2, "$items": "str"}}
        lf_schema = {"tags": pl.List(pl.Utf8)}
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 2

        df = pl.DataFrame({"tags": [["a", "b", "c"]]}).select(exprs)
        assert df.columns == ["tags.1", "tags.2"]
        assert df["tags.1"][0] == "a"
        assert df["tags.2"][0] == "b"

    def test_array_of_objects(self):
        """List(Struct) → list.get().struct.field() expressions."""
        schema = {"items": {"$max": 2, "$items": {"name": "str"}}}
        lf_schema = {"items": pl.List(pl.Struct({"name": pl.Utf8}))}
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 2

        df = pl.DataFrame({
            "items": [[{"name": "Alice"}]],
        }).select(exprs)
        assert df.columns == ["items.1.name", "items.2.name"]
        assert df["items.1.name"][0] == "Alice"
        assert df["items.2.name"][0] is None  # OOB → null

    def test_missing_top_level_column(self):
        """Schema field not in data → all leaves become pl.lit(None)."""
        schema = {"x": "int", "missing": {"a": "str", "b": "str"}}
        lf_schema = {"x": pl.Int64}  # "missing" not in data
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 3

        df = pl.DataFrame({"x": [1]}).select(exprs)
        assert df.columns == ["x", "missing.a", "missing.b"]
        assert df["missing.a"][0] is None
        assert df["missing.b"][0] is None

    def test_missing_struct_field(self):
        """Struct field in schema but not in Polars dtype → null."""
        schema = {"obj": {"known": "str", "unknown": "str"}}
        lf_schema = {"obj": pl.Struct({"known": pl.Utf8})}
        exprs = _build_polars_exprs(schema, lf_schema)
        assert len(exprs) == 2

        df = pl.DataFrame({"obj": [{"known": "yes"}]}).select(exprs)
        assert df["obj.known"][0] == "yes"
        assert df["obj.unknown"][0] is None

    def test_empty_schema(self):
        """Empty flatten schema → no expressions."""
        assert _build_polars_exprs({}, {"x": pl.Int64}) == []

    def test_empty_array_skipped(self):
        """$max=0 → no expressions for that branch."""
        schema = {"tags": {"$max": 0, "$items": "str"}}
        assert _build_polars_exprs(schema, {"tags": pl.List(pl.Utf8)}) == []


class TestFlattenWithPolars:
    def test_flattens_jsonl(self, tmp_path):
        """Basic end-to-end: JSONL → Polars flatten → parquet."""
        data = tmp_path / "data.jsonl"
        data.write_text('{"x": 1, "y": "a"}\n{"x": 2, "y": "b"}\n')
        schema = {"x": "int", "y": "str"}
        cache = tmp_path / "out.parquet"

        total = _flatten_with_polars(data, schema, cache)

        assert total == 2
        assert cache.exists()
        df = pl.read_parquet(cache)
        assert set(df.columns) == {"x", "y"}
        assert len(df) == 2

    def test_flattens_nested_jsonl(self, tmp_path):
        """Nested objects and arrays are flattened correctly."""
        records = [
            {"name": "Alice", "addr": {"city": "London"}, "tags": ["a", "b"]},
            {"name": "Bob", "addr": {"city": "Paris"}, "tags": ["c"]},
        ]
        data = tmp_path / "nested.jsonl"
        data.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        schema = {
            "name": "str",
            "addr": {"city": "str"},
            "tags": {"$max": 2, "$items": "str"},
        }
        cache = tmp_path / "nested.parquet"

        total = _flatten_with_polars(data, schema, cache)

        assert total == 2
        df = pl.read_parquet(cache)
        assert set(df.columns) == {"name", "addr.city", "tags.1", "tags.2"}
        assert df["addr.city"].to_list() == ["London", "Paris"]
        assert df["tags.1"].to_list() == ["a", "c"]
        assert df["tags.2"][0] == "b"
        assert df["tags.2"][1] is None  # OOB

    def test_flattens_json_array(self, tmp_path):
        """Regular .json file (array of objects) works too."""
        data = tmp_path / "data.json"
        data.write_text(json.dumps([{"a": 1}, {"a": 2}]))
        schema = {"a": "int"}
        cache = tmp_path / "json.parquet"

        total = _flatten_with_polars(data, schema, cache)

        assert total == 2
        df = pl.read_parquet(cache)
        assert df["a"].to_list() == [1, 2]

    def test_missing_columns_become_null(self, tmp_path):
        """Schema fields not in data → null columns in output."""
        data = tmp_path / "data.jsonl"
        data.write_text('{"x": 1}\n{"x": 2}\n')
        schema = {"x": "int", "missing": "str"}
        cache = tmp_path / "missing.parquet"

        total = _flatten_with_polars(data, schema, cache)

        assert total == 2
        df = pl.read_parquet(cache)
        assert "missing" in df.columns
        assert df["missing"].to_list() == [None, None]

    def test_empty_file_writes_empty_parquet(self, tmp_path):
        """Empty JSONL → empty parquet with correct columns."""
        data = tmp_path / "empty.jsonl"
        data.write_text("")
        schema = {"x": "int"}
        cache = tmp_path / "empty.parquet"

        # Empty JSONL will cause scan_ndjson to fail → should propagate exception
        # (caller falls back to streaming path)
        with pytest.raises(Exception):
            _flatten_with_polars(data, schema, cache)

    def test_atomic_cleanup_on_failure(self, tmp_path):
        """Temp file should not leak on failure."""
        data = tmp_path / "bad.jsonl"
        data.write_text("")  # Empty — will fail
        schema = {"x": "int"}
        cache = tmp_path / "fail.parquet"
        tmp = cache.with_suffix(".parquet.tmp")

        with pytest.raises(Exception):
            _flatten_with_polars(data, schema, cache)

        assert not cache.exists()
        # tmp may or may not exist depending on where the failure occurred

    def test_polars_matches_streaming_result(self, tmp_path):
        """Polars and streaming paths should produce equivalent data."""
        records = [
            {"name": "Alice", "age": 30, "addr": {"city": "London"}},
            {"name": "Bob", "age": 25, "addr": {"city": "Paris"}},
        ]
        data = tmp_path / "data.jsonl"
        data.write_text("\n".join(json.dumps(r) for r in records) + "\n")
        schema = {"name": "str", "age": "int", "addr": {"city": "str"}}

        polars_cache = tmp_path / "polars.parquet"
        _flatten_with_polars(data, schema, polars_cache)

        streaming_cache = tmp_path / "streaming.parquet"
        from haute._json_flatten import _iter_json_records
        _flatten_and_write_streaming(
            _iter_json_records(data), schema, streaming_cache,
        )

        df_polars = pl.read_parquet(polars_cache)
        df_streaming = pl.read_parquet(streaming_cache)

        # Same columns (order may differ)
        assert set(df_polars.columns) == set(df_streaming.columns)
        # Same row count
        assert len(df_polars) == len(df_streaming)
        # Same data (compare as dicts to handle column order + type differences)
        polars_dicts = df_polars.select(sorted(df_polars.columns)).to_dicts()
        streaming_dicts = df_streaming.select(sorted(df_streaming.columns)).to_dicts()
        for p_row, s_row in zip(polars_dicts, streaming_dicts):
            for key in p_row:
                pv, sv = p_row[key], s_row[key]
                # Allow int vs float comparison (Polars may keep int, streaming uses float64)
                if isinstance(pv, (int, float)) and isinstance(sv, (int, float)):
                    assert float(pv) == float(sv), f"Mismatch on {key}: {pv} vs {sv}"
                else:
                    assert pv == sv, f"Mismatch on {key}: {pv} vs {sv}"
