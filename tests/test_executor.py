"""Tests for haute.executor - graph execution engine."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from haute.executor import _build_node_fn, _compile_preamble, _exec_user_code, execute_graph, execute_sink
from tests.conftest import (
    make_edge as _edge,
    make_graph as _g,
    make_node as _n,
    make_output_node as _output_node,
    make_source_node as _source_node,
    make_transform_node as _transform_node,
)

# ---------------------------------------------------------------------------
# _exec_user_code
# ---------------------------------------------------------------------------

class TestExecUserCode:
    def test_chain_syntax(self):
        lf = pl.DataFrame({"x": [1, 2, 3]}).lazy()
        result = _exec_user_code(".with_columns(y=pl.col('x') * 2)", ["df"], (lf,))
        df = result.collect()
        assert "y" in df.columns
        assert df["y"].to_list() == [2, 4, 6]

    def test_full_expression(self):
        lf = pl.DataFrame({"x": [10]}).lazy()
        result = _exec_user_code("df.with_columns(y=pl.lit(42))", ["df"], (lf,))
        df = result.collect()
        assert df["y"].to_list() == [42]

    def test_assignment_style(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _exec_user_code("df = df.with_columns(y=pl.lit(99))", ["df"], (lf,))
        df = result.collect()
        assert df["y"].to_list() == [99]

    def test_syntax_error_adjusts_line_number(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        with pytest.raises(SyntaxError):
            _exec_user_code(".invalid syntax here !!!", ["df"], (lf,))

    def test_eager_result_converted_to_lazy(self):
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _exec_user_code("df = df.collect()", ["df"], (lf,))
        assert isinstance(result, pl.LazyFrame)

    def test_multiple_named_sources(self):
        """Multiple source names are injected as named variables."""
        lf_a = pl.DataFrame({"x": [1]}).lazy()
        lf_b = pl.DataFrame({"y": [2]}).lazy()
        code = "df = a.join(b, how='cross')"
        result = _exec_user_code(code, ["a", "b"], (lf_a, lf_b))
        df = result.collect()
        assert set(df.columns) == {"x", "y"}

    def test_extra_ns_injects_object(self):
        """extra_ns should inject additional variables (e.g. external model obj)."""
        lf = pl.DataFrame({"x": [10]}).lazy()
        code = "df = df.with_columns(y=pl.lit(val))"
        result = _exec_user_code(code, ["df"], (lf,), extra_ns={"val": 42})
        df = result.collect()
        assert df["y"].to_list() == [42]

    def test_runtime_error_preserves_message(self):
        """Runtime errors in user code should propagate with useful messages."""
        lf = pl.DataFrame({"x": [1]}).lazy()
        with pytest.raises(Exception) as exc_info:
            _exec_user_code("df = 1 / 0", ["df"], (lf,))
        assert "division" in str(exc_info.value).lower()

    def test_bare_expression_wraps_as_df_assignment(self):
        """Code without 'df =' is wrapped as df = (<code>), i.e. an expression."""
        lf = pl.DataFrame({"x": [1, 2]}).lazy()
        # A bare expression (no df = ...) gets wrapped as df = (expression)
        result = _exec_user_code("df.with_columns(y=pl.lit(7))", ["df"], (lf,))
        df = result.collect()
        assert "y" in df.columns
        assert df["y"].to_list() == [7, 7]

    def test_explicit_df_assignment_preserved(self):
        """When user code uses 'df = ...', no extra wrapping happens."""
        lf = pl.DataFrame({"x": [1]}).lazy()
        result = _exec_user_code(
            "df = df.with_columns(y=pl.lit(99))",
            ["df"], (lf,),
        )
        assert result.collect()["y"].to_list() == [99]

    def test_empty_dataframe_passthrough(self):
        """Empty DataFrame (0 rows) passes through correctly."""
        lf = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)}).lazy()
        result = _exec_user_code(".with_columns(y=pl.col('x') * 2)", ["df"], (lf,))
        df = result.collect()
        assert len(df) == 0
        assert set(df.columns) == {"x", "y"}


# ---------------------------------------------------------------------------
# _compile_preamble
# ---------------------------------------------------------------------------


class TestCompilePreamble:
    def test_empty_preamble_returns_empty_dict(self):
        assert _compile_preamble("") == {}
        assert _compile_preamble("   ") == {}

    def test_extracts_functions(self):
        ns = _compile_preamble("def double(x):\n    return x * 2\n")
        assert "double" in ns
        assert ns["double"](5) == 10

    def test_extracts_constants(self):
        ns = _compile_preamble("MY_MAP = {'a': 1}\nMY_LIST = [1, 2]\n")
        assert ns["MY_MAP"] == {"a": 1}
        assert ns["MY_LIST"] == [1, 2]

    def test_functions_can_call_each_other(self):
        code = (
            "def helper(x):\n"
            "    return x + 1\n"
            "\n"
            "def main(x):\n"
            "    return helper(x) * 2\n"
        )
        ns = _compile_preamble(code)
        assert ns["main"](3) == 8  # (3 + 1) * 2

    def test_functions_can_use_polars(self):
        ns = _compile_preamble("def make_lit():\n    return pl.lit(42)\n")
        expr = ns["make_lit"]()
        assert isinstance(expr, pl.Expr)


# ---------------------------------------------------------------------------
# _build_node_fn with preamble_ns
# ---------------------------------------------------------------------------


class TestBuildNodeFnWithPreamble:
    def test_transform_can_call_preamble_function(self):
        preamble_ns = _compile_preamble(
            "def add_ten(col):\n    return pl.col(col) + 10\n"
        )
        node = _transform_node("t", code="df = df.with_columns(y=add_ten('x'))")
        _, fn, _ = _build_node_fn(node, source_names=["df"], preamble_ns=preamble_ns)
        lf = pl.DataFrame({"x": [1, 2]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [11, 12]

    def test_transform_can_use_preamble_constant(self):
        preamble_ns = _compile_preamble("FACTOR = 3\n")
        node = _transform_node("t", code="df = df.with_columns(y=pl.col('x') * FACTOR)")
        _, fn, _ = _build_node_fn(node, source_names=["df"], preamble_ns=preamble_ns)
        lf = pl.DataFrame({"x": [5]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [15]


# ---------------------------------------------------------------------------
# _build_node_fn
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("_widen_sandbox_root")
class TestBuildNodeFn:
    @pytest.mark.parametrize(
        "ext, col, values, write_method",
        [
            pytest.param("parquet", "a", [1, 2], "write_parquet", id="parquet"),
            pytest.param("csv", "b", [3, 4], "write_csv", id="csv"),
            pytest.param("json", "c", [5, 6], "write_json", id="json"),
        ],
    )
    def test_data_source_file_formats(self, tmp_path, ext, col, values, write_method):
        p = tmp_path / f"data.{ext}"
        getattr(pl.DataFrame({col: values}), write_method)(p)

        node = _source_node("src", str(p))
        name, fn, is_source = _build_node_fn(node)
        assert is_source is True
        result = fn()
        if isinstance(result, pl.LazyFrame):
            df = result.collect()
        else:
            df = result
        assert df[col].to_list() == values

    def test_data_source_databricks_is_source(self):
        node = _n({
            "id": "db",
            "data": {
                "label": "db",
                "nodeType": "dataSource",
                "config": {"sourceType": "databricks", "table": "cat.sch.tbl"},
            },
        })
        _, fn, is_source = _build_node_fn(node)
        assert is_source is True
        # Without a cached parquet file, calling fn() raises CacheNotFoundError
        from haute._databricks_io import CacheNotFoundError

        with pytest.raises(CacheNotFoundError, match="not.*fetched"):
            fn()

    def test_transform_with_code(self):
        node = _transform_node("t", code=".with_columns(y=pl.col('x') + 1)")
        _, fn, is_source = _build_node_fn(node, source_names=["df"])
        assert is_source is False
        lf = pl.DataFrame({"x": [10]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [11]

    def test_transform_passthrough_without_code(self):
        node = _transform_node("t", code="")
        _, fn, is_source = _build_node_fn(node)
        lf = pl.DataFrame({"x": [5]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [5]

    def test_output_selects_fields(self):
        node = _output_node("out", fields=["a"])
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        df = fn(lf).collect()
        assert df.columns == ["a"]

    def test_output_passthrough_without_fields(self):
        node = _output_node("out", fields=[])
        _, fn, _ = _build_node_fn(node)
        lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        df = fn(lf).collect()
        assert set(df.columns) == {"a", "b"}

    def test_sink_passthrough(self):
        node = _n({
            "id": "sink",
            "data": {"label": "sink", "nodeType": "dataSink", "config": {}},
        })
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [1]

    def test_external_file_with_json(self, tmp_path):
        """externalFile with JSON file type loads and injects obj."""
        import json as _json
        p = tmp_path / "data.json"
        p.write_text(_json.dumps({"multiplier": 10}))

        node = _n({
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {
                    "path": str(p),
                    "fileType": "json",
                    "code": "df = df.with_columns(y=pl.lit(obj['multiplier']))",
                },
            },
        })
        _, fn, is_source = _build_node_fn(node, source_names=["df"])
        assert is_source is False
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [10]

    def test_external_file_with_pickle(self, tmp_path):
        """externalFile with pickle file type loads and injects obj."""
        import pickle
        p = tmp_path / "data.pkl"
        with open(p, "wb") as f:
            pickle.dump({"factor": 5}, f)

        node = _n({
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {
                    "path": str(p),
                    "fileType": "pickle",
                    "code": "df = df.with_columns(y=pl.lit(obj['factor']))",
                },
            },
        })
        _, fn, _ = _build_node_fn(node, source_names=["df"])
        lf = pl.DataFrame({"x": [1]}).lazy()
        df = fn(lf).collect()
        assert df["y"].to_list() == [5]

    def test_external_file_passthrough_without_code(self):
        """externalFile without code acts as passthrough."""
        node = _n({
            "id": "ext",
            "data": {
                "label": "ext",
                "nodeType": "externalFile",
                "config": {"path": "model.pkl", "fileType": "pickle", "code": ""},
            },
        })
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [7]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [7]

    def test_unknown_node_type_rejected(self):
        """Unknown nodeType should be rejected by NodeType enum validation."""
        import pytest

        with pytest.raises(Exception):
            _n({
                "id": "unk",
                "data": {"label": "unk", "nodeType": "unknownFutureType", "config": {}},
            })

    def test_unhandled_node_type_passthrough(self):
        """Node type without dedicated handler falls through to passthrough."""
        node = _n({
            "id": "port1",
            "data": {"label": "port1", "nodeType": "submodelPort", "config": {}},
        })
        _, fn, is_source = _build_node_fn(node)
        assert is_source is False
        lf = pl.DataFrame({"x": [3]}).lazy()
        df = fn(lf).collect()
        assert df["x"].to_list() == [3]


# ---------------------------------------------------------------------------
# execute_graph
# ---------------------------------------------------------------------------

class TestExecuteGraph:
    def test_simple_pipeline(self, tmp_path):
        p = tmp_path / "input.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", ".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "t")],
        })
        results = execute_graph(graph)
        assert results["src"].status == "ok"
        assert results["t"].status == "ok"
        assert results["t"].row_count == 3
        assert any(c.name == "y" for c in results["t"].columns)

    def test_timing_ms_present(self, tmp_path):
        p = tmp_path / "t.parquet"
        pl.DataFrame({"x": [1, 2]}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", ".with_columns(y=pl.col('x') + 1)"),
            ],
            "edges": [_edge("src", "t")],
        })
        results = execute_graph(graph)
        for nid in ("src", "t"):
            assert isinstance(results[nid].timing_ms, float)
            assert results[nid].timing_ms >= 0

    def test_empty_graph(self):
        assert execute_graph(_g({"nodes": [], "edges": []})) == {}

    def test_row_limit(self, tmp_path):
        p = tmp_path / "big.parquet"
        pl.DataFrame({"x": list(range(100))}).write_parquet(p)

        graph = _g({"nodes": [_source_node("s", str(p))], "edges": []})
        results = execute_graph(graph, row_limit=5)
        assert results["s"].row_count == 5

    def test_target_node_id(self, tmp_path):
        p = tmp_path / "d.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("a", str(p)),
                _transform_node("b"),
                _transform_node("c"),
            ],
            "edges": [_edge("a", "b"), _edge("b", "c")],
        })
        results = execute_graph(graph, target_node_id="b")
        assert "b" in results
        assert "c" not in results

    def test_error_node_captured(self, tmp_path):
        p = tmp_path / "d.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("src", str(p)),
                # Select a column that doesn't exist - triggers ColumnNotFoundError at collect
                _transform_node("bad", code=".select('nonexistent_col')"),
            ],
            "edges": [_edge("src", "bad")],
        })
        results = execute_graph(graph)
        assert results["bad"].status == "error"
        assert "nonexistent_col" in results["bad"].error.lower() or "not found" in results["bad"].error.lower(), (
            f"Expected column-not-found error, got: {results['bad'].error}"
        )
        assert results["bad"].row_count == 0
        assert results["bad"].columns == []

    def test_cascading_failure_propagates(self, tmp_path):
        """When a mid-pipeline node fails, downstream nodes also fail."""
        p = tmp_path / "d.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("mid", code=".select('nonexistent_col')"),
                _transform_node("leaf", code=".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "mid"), _edge("mid", "leaf")],
        })
        results = execute_graph(graph)
        # Mid node should fail
        assert results["mid"].status == "error"
        # Leaf node must also fail (it received None input from the failed mid node)
        assert results["leaf"].status == "error"
        assert results["leaf"].row_count == 0
        # Source should still succeed
        assert results["src"].status == "ok"

    def test_row_limit_zero_is_no_limit(self, tmp_path):
        """row_limit=0 is falsy, so it behaves the same as None (no limit)."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": list(range(10))}).write_parquet(p)

        graph = _g({
            "nodes": [_source_node("src", str(p))],
            "edges": [],
        })
        results = execute_graph(graph, row_limit=0)
        assert results["src"].status == "ok"
        assert results["src"].row_count == 10

    def test_row_limit_one(self, tmp_path):
        """row_limit=1 should produce exactly 1 row through the pipeline."""
        p = tmp_path / "data.parquet"
        pl.DataFrame({"x": list(range(10))}).write_parquet(p)

        graph = _g({
            "nodes": [
                _source_node("src", str(p)),
                _transform_node("t", code=".with_columns(y=pl.col('x') * 2)"),
            ],
            "edges": [_edge("src", "t")],
        })
        results = execute_graph(graph, row_limit=1)
        assert results["src"].row_count == 1
        assert results["t"].row_count == 1
        col_names = [c.name for c in results["t"].columns]
        assert "x" in col_names
        assert "y" in col_names

    def test_empty_source_dataframe(self, tmp_path):
        """A source file with 0 rows should still have schema metadata."""
        p = tmp_path / "empty.parquet"
        pl.DataFrame({"a": pl.Series([], dtype=pl.Int64), "b": pl.Series([], dtype=pl.Utf8)}).write_parquet(p)

        graph = _g({"nodes": [_source_node("src", str(p))], "edges": []})
        results = execute_graph(graph)
        assert results["src"].status == "ok"
        assert results["src"].row_count == 0
        col_names = [c.name for c in results["src"].columns]
        assert "a" in col_names
        assert "b" in col_names


# ---------------------------------------------------------------------------
# execute_sink
# ---------------------------------------------------------------------------

class TestExecuteSink:
    def test_writes_parquet(self, tmp_path):
        src_path = tmp_path / "in.parquet"
        out_path = tmp_path / "out.parquet"
        pl.DataFrame({"x": [1, 2]}).write_parquet(src_path)

        graph = _g({
            "nodes": [
                _source_node("src", str(src_path)),
                _n({
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": str(out_path), "format": "parquet"},
                    },
                }),
            ],
            "edges": [_edge("src", "sink")],
        })
        result = execute_sink(graph, sink_node_id="sink")
        assert result.status == "ok"
        assert result.row_count == 2
        assert out_path.exists()
        df = pl.read_parquet(out_path)
        assert len(df) == 2

    def test_writes_csv(self, tmp_path):
        src_path = tmp_path / "in.parquet"
        out_path = tmp_path / "out.csv"
        pl.DataFrame({"a": [10]}).write_parquet(src_path)

        graph = _g({
            "nodes": [
                _source_node("src", str(src_path)),
                _n({
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": str(out_path), "format": "csv"},
                    },
                }),
            ],
            "edges": [_edge("src", "sink")],
        })
        result = execute_sink(graph, sink_node_id="sink")
        assert result.status == "ok"
        assert out_path.exists()

    def test_missing_sink_raises(self):
        graph = _g({"nodes": [], "edges": []})
        with pytest.raises(ValueError, match="not.*found"):
            execute_sink(graph, sink_node_id="nope")

    def test_no_path_raises(self, tmp_path):
        src = tmp_path / "in.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(src)
        graph = _g({
            "nodes": [
                _source_node("src", str(src)),
                _n({
                    "id": "sink",
                    "data": {
                        "label": "sink",
                        "nodeType": "dataSink",
                        "config": {"path": "", "format": "parquet"},
                    },
                }),
            ],
            "edges": [_edge("src", "sink")],
        })
        with pytest.raises(ValueError, match="no.*output path"):
            execute_sink(graph, sink_node_id="sink")


# ---------------------------------------------------------------------------
# Instance node alias injection
# ---------------------------------------------------------------------------

class TestInstanceAliasInjection:
    """Regression tests for instance node input mapping."""

    def test_positional_fallback_maps_unrelated_name(self):
        """Instance input 'instance' must bind as 'claims_aggregate' via
        positional fallback when no name-based match exists."""
        lf = pl.DataFrame({"IDpol": [1, 2], "val": [10, 20]}).lazy()
        result = _exec_user_code(
            "df = claims_aggregate",
            ["instance", "policies"],
            (lf, lf),
            orig_source_names=["claims_aggregate", "policies"],
        )
        assert result.collect().columns == ["IDpol", "val"]

    def test_explicit_mapping_overrides_heuristic(self):
        """Explicit inputMapping takes priority over name matching."""
        lf_a = pl.DataFrame({"x": [1]}).lazy()
        lf_b = pl.DataFrame({"y": [2]}).lazy()
        result = _exec_user_code(
            "df = orig_input",
            ["src_a", "src_b"],
            (lf_a, lf_b),
            orig_source_names=["orig_input"],
            input_mapping={"orig_input": "src_b"},
        )
        assert result.collect().columns == ["y"]

    def test_instance_node_executes_with_different_input_names(self, tmp_path):
        """End-to-end: instance of a transform node runs correctly when its
        upstream inputs have completely different names from the original's."""
        # Write small parquet files as data sources
        src_a = tmp_path / "a.parquet"
        src_b = tmp_path / "b.parquet"
        alt_a = tmp_path / "alt_a.parquet"
        alt_b = tmp_path / "alt_b.parquet"
        pl.DataFrame({"k": [1, 2], "v": [10, 20]}).write_parquet(src_a)
        pl.DataFrame({"k": [1, 2], "w": [30, 40]}).write_parquet(src_b)
        pl.DataFrame({"k": [3], "v": [50]}).write_parquet(alt_a)
        pl.DataFrame({"k": [3], "w": [60]}).write_parquet(alt_b)

        graph = _g({
            "nodes": [
                _source_node("src_a", str(src_a)),
                _source_node("src_b", str(src_b)),
                _transform_node("joiner", "df = src_a.join(src_b, on='k')"),
                _source_node("alt_a", str(alt_a)),
                _source_node("alt_b", str(alt_b)),
                _n({
                    "id": "joiner_inst",
                    "data": {
                        "label": "joiner_inst",
                        "nodeType": "transform",
                        "config": {"instanceOf": "joiner"},
                    },
                }),
            ],
            "edges": [
                _edge("src_a", "joiner"),
                _edge("src_b", "joiner"),
                _edge("alt_a", "joiner_inst"),
                _edge("alt_b", "joiner_inst"),
            ],
        })
        results = execute_graph(graph, target_node_id="joiner_inst")
        assert results["joiner_inst"].status == "ok"
        assert results["joiner_inst"].row_count == 1
        col_names = {c.name for c in results["joiner_inst"].columns}
        assert col_names == {"k", "v", "w"}


# ---------------------------------------------------------------------------
# liveSwitch node
# ---------------------------------------------------------------------------


class TestLiveSwitch:
    def _switch_graph(self, tmp_path, mode="live", reverse_edges=False):
        """Build a graph with two sources feeding a liveSwitch."""
        p1 = tmp_path / "live.parquet"
        p2 = tmp_path / "batch.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p1)
        pl.DataFrame({"x": [10, 20, 30, 40]}).write_parquet(p2)
        edges = [
            _edge("live_src", "switch"),
            _edge("batch_src", "switch"),
        ]
        if reverse_edges:
            edges = list(reversed(edges))
        return _g({
            "nodes": [
                _source_node("live_src", str(p1)),
                _source_node("batch_src", str(p2)),
                _n({
                    "id": "switch",
                    "type": "liveSwitch",
                    "data": {
                        "label": "switch",
                        "nodeType": "liveSwitch",
                        "config": {
                            "mode": mode,
                            "inputs": ["live_src", "batch_src"],
                        },
                    },
                    "position": {"x": 0, "y": 0},
                }),
            ],
            "edges": edges,
        })

    def test_live_mode_selects_first_input(self, tmp_path):
        graph = self._switch_graph(tmp_path, mode="live")
        results = execute_graph(graph, target_node_id="switch")
        assert results["switch"].status == "ok"
        assert results["switch"].row_count == 3

    def test_live_mode_works_regardless_of_edge_order(self, tmp_path):
        """Edge order in the graph JSON is arbitrary — live must still pick the correct input."""
        graph = self._switch_graph(tmp_path, mode="live", reverse_edges=True)
        results = execute_graph(graph, target_node_id="switch")
        assert results["switch"].status == "ok"
        assert results["switch"].row_count == 3

    def test_batch_mode_selects_named_input(self, tmp_path):
        graph = self._switch_graph(tmp_path, mode="batch_src")
        results = execute_graph(graph, target_node_id="switch")
        assert results["switch"].status == "ok"
        assert results["switch"].row_count == 4


# ---------------------------------------------------------------------------
# API Input large-file gating
# ---------------------------------------------------------------------------


def _api_input_node(nid: str, path: str) -> _n:
    """Build a minimal apiInput node."""
    return _n({
        "id": nid,
        "data": {
            "label": nid,
            "nodeType": "apiInput",
            "config": {"path": path},
        },
    })


class TestApiInputLargeFileGating:
    def test_large_file_no_cache_raises_error(self, tmp_path, monkeypatch):
        """Large JSONL files without a cache should produce a descriptive error."""
        from haute._json_flatten import _LARGE_FILE_THRESHOLD

        monkeypatch.chdir(tmp_path)

        # Create a "large" JSONL file that exceeds the threshold
        data_file = tmp_path / "large.jsonl"
        # Write enough valid JSONL lines to exceed threshold
        line = json.dumps({"x": "a" * 1000}) + "\n"
        lines_needed = (_LARGE_FILE_THRESHOLD // len(line)) + 1
        data_file.write_text(line * lines_needed)
        assert data_file.stat().st_size >= _LARGE_FILE_THRESHOLD

        node = _api_input_node("api", str(data_file))
        _, fn, is_source = _build_node_fn(node)
        assert is_source is True

        with pytest.raises(RuntimeError, match="not been cached yet"):
            fn()

    def test_large_file_with_cache_succeeds(self, tmp_path, monkeypatch):
        """Large JSONL files with a valid cache should use the cache directly."""
        from haute._json_flatten import _LARGE_FILE_THRESHOLD, _json_cache_path

        monkeypatch.chdir(tmp_path)

        # Create a "large" JSONL file
        data_file = tmp_path / "large.jsonl"
        line = json.dumps({"x": 1}) + "\n"
        lines_needed = (_LARGE_FILE_THRESHOLD // len(line)) + 1
        data_file.write_text(line * lines_needed)
        assert data_file.stat().st_size >= _LARGE_FILE_THRESHOLD

        # Pre-build the cache manually
        cache_path = _json_cache_path(str(data_file))
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(cache_path)

        node = _api_input_node("api", str(data_file))
        _, fn, _ = _build_node_fn(node)
        result = fn()
        df = result.collect()
        assert df["x"].to_list() == [1, 2, 3]

    def test_small_file_auto_processes(self, tmp_path, monkeypatch):
        """Small JSONL files should auto-process without requiring explicit caching."""
        monkeypatch.chdir(tmp_path)

        data_file = tmp_path / "small.jsonl"
        data_file.write_text('{"x": 10}\n{"x": 20}\n')

        node = _api_input_node("api", str(data_file))
        _, fn, _ = _build_node_fn(node)
        result = fn()
        df = result.collect()
        assert df["x"].to_list() == [10, 20]
