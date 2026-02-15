"""Tests for parser internals — unit tests for extraction and config building."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from haute.parser import (
    _build_node_config,
    _dedent,
    _extract_external_user_code,
    _extract_preamble,
    _extract_user_code,
    _infer_node_type,
    _strip_docstring,
    parse_pipeline_file,
    parse_pipeline_source,
)


# ---------------------------------------------------------------------------
# _infer_node_type
# ---------------------------------------------------------------------------

class TestInferNodeType:
    def test_external_file(self):
        assert _infer_node_type({"external": "model.pkl"}, 1) == "externalFile"

    def test_data_sink(self):
        assert _infer_node_type({"sink": "out.parquet"}, 1) == "dataSink"

    def test_output(self):
        assert _infer_node_type({"output": True}, 1) == "output"

    def test_model_score(self):
        assert _infer_node_type({"model_uri": "models:/m/1"}, 1) == "modelScore"

    def test_rating_step(self):
        assert _infer_node_type({"table": "t", "key": "k"}, 1) == "ratingStep"

    def test_data_source_by_path(self):
        assert _infer_node_type({"path": "data.parquet"}, 0) == "dataSource"

    def test_data_source_by_zero_params(self):
        assert _infer_node_type({}, 0) == "dataSource"

    def test_transform_default(self):
        assert _infer_node_type({}, 1) == "transform"

    def test_priority_external_over_path(self):
        """external takes priority even if path is present."""
        assert _infer_node_type({"external": "m.pkl", "path": "x"}, 1) == "externalFile"


# ---------------------------------------------------------------------------
# _strip_docstring
# ---------------------------------------------------------------------------

class TestStripDocstring:
    def test_single_line_docstring(self):
        lines = ['    """This is a docstring."""', "    return df"]
        result = _strip_docstring(lines)
        assert result == ["    return df"]

    def test_multi_line_docstring(self):
        # Closing triple-quote shares line with content (common in codegen output)
        lines = [
            '    """First line.',
            '    Second line."""',
            "    return df",
        ]
        result = _strip_docstring(lines)
        assert result == ["    return df"]

    def test_standalone_closing_triple_quote(self):
        # Closing triple-quote on its own line (standard Python docstring style)
        lines = [
            '    """First line.',
            "    Second line.",
            '    """',
            "    return df",
        ]
        result = _strip_docstring(lines)
        assert result == ["    return df"]

    def test_no_docstring(self):
        lines = ["    x = 1", "    return x"]
        result = _strip_docstring(lines)
        assert result == ["    x = 1", "    return x"]

    def test_empty_input(self):
        assert _strip_docstring([]) == []


# ---------------------------------------------------------------------------
# _dedent
# ---------------------------------------------------------------------------

class TestDedent:
    def test_removes_common_indent(self):
        code = "    x = 1\n    y = 2"
        assert _dedent(code) == "x = 1\ny = 2"

    def test_preserves_relative_indent(self):
        code = "    if True:\n        x = 1"
        assert _dedent(code) == "if True:\n    x = 1"

    def test_empty_string(self):
        assert _dedent("") == ""

    def test_no_indent(self):
        assert _dedent("x = 1\ny = 2") == "x = 1\ny = 2"


# ---------------------------------------------------------------------------
# _extract_user_code
# ---------------------------------------------------------------------------

class TestExtractUserCode:
    def test_codegen_style_df_assignment(self):
        """Codegen produces: df = (\n    source\n    .filter(...)\n)\nreturn df"""
        body = '    """doc"""\n    df = (\n        source\n        .filter(pl.col("x") > 0)\n    )\n    return df'
        result = _extract_user_code(body, ["source"])
        assert "source" in result
        assert ".filter" in result
        assert "return" not in result
        assert "df =" not in result

    def test_single_return_expression(self):
        body = '    """doc"""\n    return source.with_columns(y=pl.lit(1))'
        result = _extract_user_code(body, ["source"])
        assert "source.with_columns" in result
        assert "return" not in result

    def test_chain_syntax(self):
        body = '    """doc"""\n    df = (\n        df\n        .filter(pl.col("x") > 0)\n    )\n    return df'
        result = _extract_user_code(body, ["df"])
        assert ".filter" in result

    def test_empty_body(self):
        assert _extract_user_code("", ["df"]) == ""

    def test_docstring_only(self):
        body = '    """Just a docstring."""'
        result = _extract_user_code(body, ["df"])
        # After stripping docstring, nothing left
        assert result == ""


# ---------------------------------------------------------------------------
# _extract_external_user_code
# ---------------------------------------------------------------------------

class TestExtractExternalUserCode:
    def test_strips_import_and_with_block(self):
        body = (
            '    """doc"""\n'
            "    import pickle\n"
            '    with open("model.pkl", "rb") as _f:\n'
            "        obj = pickle.load(_f)\n"
            "    df = df.with_columns(pred=pl.lit(obj.predict()))\n"
            "    return df"
        )
        result = _extract_external_user_code(body, ["df"])
        assert "df = df.with_columns" in result
        assert "import pickle" not in result
        assert "with open" not in result
        assert "return df" not in result

    def test_strips_obj_assignment(self):
        body = (
            '    """doc"""\n'
            "    import joblib\n"
            '    obj = joblib.load("model.pkl")\n'
            "    df = df.with_columns(score=pl.lit(42))\n"
            "    return df"
        )
        result = _extract_external_user_code(body, ["df"])
        assert "score" in result
        assert "joblib" not in result

    def test_empty_body(self):
        assert _extract_external_user_code("", ["df"]) == ""

    def test_only_boilerplate_returns_empty(self):
        body = (
            '    """doc"""\n'
            "    import pickle\n"
            '    with open("m.pkl", "rb") as f:\n'
            "        obj = pickle.load(f)\n"
            "    return df"
        )
        result = _extract_external_user_code(body, ["df"])
        assert result == ""


# ---------------------------------------------------------------------------
# _build_node_config
# ---------------------------------------------------------------------------

class TestBuildNodeConfig:
    def test_data_source_flat_file(self):
        config = _build_node_config("dataSource", {"path": "d.parquet"}, "", [])
        assert config["path"] == "d.parquet"
        assert config["sourceType"] == "flat_file"

    def test_data_source_databricks(self):
        config = _build_node_config(
            "dataSource", {"table": "catalog.schema.tbl"}, "", [],
        )
        assert config["sourceType"] == "databricks"
        assert config["table"] == "catalog.schema.tbl"

    def test_model_score(self):
        config = _build_node_config("modelScore", {"model_uri": "m/1"}, "", ["df"])
        assert config["model_uri"] == "m/1"

    def test_rating_step(self):
        config = _build_node_config("ratingStep", {"table": "t", "key": "k"}, "", ["df"])
        assert config["table"] == "t"
        assert config["key"] == "k"

    def test_data_sink(self):
        config = _build_node_config("dataSink", {"sink": "out.csv", "format": "csv"}, "", ["df"])
        assert config["path"] == "out.csv"
        assert config["format"] == "csv"

    def test_external_file(self):
        config = _build_node_config(
            "externalFile",
            {"external": "model.pkl", "file_type": "pickle"},
            "",
            ["df"],
        )
        assert config["path"] == "model.pkl"
        assert config["fileType"] == "pickle"

    def test_external_file_catboost(self):
        config = _build_node_config(
            "externalFile",
            {"external": "m.cbm", "file_type": "catboost", "model_class": "regressor"},
            "",
            ["df"],
        )
        assert config["fileType"] == "catboost"
        assert config["modelClass"] == "regressor"

    def test_output(self):
        config = _build_node_config("output", {"fields": ["a", "b"]}, "", ["df"])
        assert config["fields"] == ["a", "b"]

    def test_transform(self):
        body = '    """doc"""\n    return df'
        config = _build_node_config("transform", {}, body, ["df"])
        assert "code" in config


# ---------------------------------------------------------------------------
# _extract_preamble
# ---------------------------------------------------------------------------

class TestExtractPreamble:
    def test_extracts_between_imports_and_pipeline(self):
        source = (
            "import polars as pl\n"
            "import haute\n"
            "\n"
            "from pathlib import Path\n"
            "DATA = 42\n"
            "\n"
            'pipeline = haute.Pipeline("test")\n'
        )
        preamble = _extract_preamble(source)
        assert "from pathlib import Path" in preamble
        assert "DATA = 42" in preamble
        assert "import polars" not in preamble
        assert "Pipeline" not in preamble

    def test_no_preamble(self):
        source = (
            "import polars as pl\n"
            "import haute\n"
            "\n"
            'pipeline = haute.Pipeline("test")\n'
        )
        preamble = _extract_preamble(source)
        assert preamble == ""

    def test_no_standard_imports(self):
        source = 'pipeline = haute.Pipeline("test")\n'
        assert _extract_preamble(source) == ""


# ---------------------------------------------------------------------------
# _fallback_parse (syntax error path)
# ---------------------------------------------------------------------------

class TestFallbackParse:
    def test_parses_despite_syntax_error(self, tmp_path):
        """File with syntax error in one function should still parse other nodes."""
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("broken")


@pipeline.node(path="data.parquet")
def source() -> pl.DataFrame:
    """Good node."""
    return pl.scan_parquet("data.parquet")


@pipeline.node
def bad_node(df: pl.DataFrame) -> pl.DataFrame:
    """This has a syntax error."""
    return df.with_columns(
        # missing closing paren


@pipeline.node
def good_node(df: pl.DataFrame) -> pl.DataFrame:
    """This is fine."""
    return df
'''
        p = tmp_path / "broken.py"
        p.write_text(code)
        graph = parse_pipeline_file(p)

        # Should still extract nodes despite syntax error
        assert graph["pipeline_name"] == "broken"
        assert "warning" in graph
        assert len(graph["nodes"]) >= 2  # at least source and good_node

    def test_fallback_extracts_edges(self, tmp_path):
        """Regex fallback should find pipeline.connect() calls."""
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("edges")


@pipeline.node
def a() -> pl.DataFrame:
    return pl.DataFrame(

@pipeline.node
def b(a: pl.DataFrame) -> pl.DataFrame:
    return a

pipeline.connect("a", "b")
'''
        p = tmp_path / "edges.py"
        p.write_text(code)
        graph = parse_pipeline_file(p)
        edge_pairs = [(e["source"], e["target"]) for e in graph["edges"]]
        assert ("a", "b") in edge_pairs


# ---------------------------------------------------------------------------
# Roundtrip: parse → codegen → parse (deep comparison)
# ---------------------------------------------------------------------------

class TestDeepRoundtrip:
    def test_roundtrip_preserves_node_types_and_configs(self, tmp_path):
        from haute.codegen import graph_to_code

        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("roundtrip", description="Test pipeline")


@pipeline.node(path="data.parquet")
def source() -> pl.DataFrame:
    """Load data."""
    return pl.scan_parquet("data.parquet")


@pipeline.node
def transform(source: pl.DataFrame) -> pl.DataFrame:
    """Clean data."""
    df = (
        source
        .filter(pl.col("x") > 0)
    )
    return df


@pipeline.node(output=True, fields=["x", "y"])
def output(transform: pl.DataFrame) -> pl.DataFrame:
    """Final output."""
    return transform.select("x", "y")


pipeline.connect("source", "transform")
pipeline.connect("transform", "output")
'''
        p = tmp_path / "original.py"
        p.write_text(code)
        graph1 = parse_pipeline_file(p)

        generated = graph_to_code(
            graph1,
            pipeline_name=graph1["pipeline_name"],
            description=graph1.get("pipeline_description", ""),
            preamble=graph1.get("preamble", ""),
        )
        p2 = tmp_path / "generated.py"
        p2.write_text(generated)
        graph2 = parse_pipeline_file(p2)

        # Same node count
        assert len(graph1["nodes"]) == len(graph2["nodes"])

        # Same node types
        types1 = {n["id"]: n["data"]["nodeType"] for n in graph1["nodes"]}
        types2 = {n["id"]: n["data"]["nodeType"] for n in graph2["nodes"]}
        assert types1 == types2

        # Same edge pairs
        edges1 = {(e["source"], e["target"]) for e in graph1["edges"]}
        edges2 = {(e["source"], e["target"]) for e in graph2["edges"]}
        assert edges1 == edges2

        # Source config preserved
        src1 = next(n for n in graph1["nodes"] if n["id"] == "source")
        src2 = next(n for n in graph2["nodes"] if n["id"] == "source")
        assert src1["data"]["config"]["path"] == src2["data"]["config"]["path"]

        # Output fields preserved
        out1 = next(n for n in graph1["nodes"] if n["id"] == "output")
        out2 = next(n for n in graph2["nodes"] if n["id"] == "output")
        assert out1["data"]["config"].get("fields") == out2["data"]["config"].get("fields")

        # Pipeline name preserved
        assert graph1["pipeline_name"] == graph2["pipeline_name"]
