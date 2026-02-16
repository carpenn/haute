"""Tests for haute.parser - .py pipeline file → React Flow graph JSON."""

from __future__ import annotations

from pathlib import Path

import pytest

from haute.parser import parse_pipeline_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_pipeline(tmp_path: Path, code: str) -> Path:
    """Write a pipeline .py file and return its path."""
    p = tmp_path / "test_pipeline.py"
    p.write_text(code)
    return p


# ---------------------------------------------------------------------------
# Basic parsing
# ---------------------------------------------------------------------------

class TestParsePipelineFile:
    def test_simple_pipeline(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("test", description="A test pipeline")


@pipeline.node(path="data.parquet")
def load_data() -> pl.DataFrame:
    """Load input data."""
    return pl.scan_parquet("data.parquet")


@pipeline.node
def transform(load_data: pl.DataFrame) -> pl.DataFrame:
    """Transform the data."""
    return load_data


pipeline.connect("load_data", "transform")
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)

        assert graph["pipeline_name"] == "test"
        assert len(graph["nodes"]) == 2
        assert len(graph["edges"]) >= 1

        # Check node types inferred correctly
        node_map = {n["id"]: n for n in graph["nodes"]}
        assert node_map["load_data"]["data"]["nodeType"] == "dataSource"
        assert node_map["transform"]["data"]["nodeType"] == "transform"

    def test_pipeline_name_extracted(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("my_pricing", description="Motor pricing")
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        assert graph["pipeline_name"] == "my_pricing"

    def test_edges_from_connect_calls(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("edges_test")


@pipeline.node
def a() -> pl.DataFrame:
    return pl.DataFrame()


@pipeline.node
def b(a: pl.DataFrame) -> pl.DataFrame:
    return a


pipeline.connect("a", "b")
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        edge_pairs = [(e["source"], e["target"]) for e in graph["edges"]]
        assert ("a", "b") in edge_pairs

    def test_implicit_edges_from_param_names(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("implicit")


@pipeline.node
def source() -> pl.DataFrame:
    return pl.DataFrame()


@pipeline.node
def transform(source: pl.DataFrame) -> pl.DataFrame:
    return source
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        edge_pairs = [(e["source"], e["target"]) for e in graph["edges"]]
        assert ("source", "transform") in edge_pairs

    def test_node_config_extracted(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("config_test")


@pipeline.node(path="data/input.parquet")
def load_data() -> pl.DataFrame:
    """Read the data."""
    return pl.scan_parquet("data/input.parquet")
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        node = graph["nodes"][0]
        assert node["data"]["config"]["path"] == "data/input.parquet"

    def test_docstring_as_description(self, tmp_path):
        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("doc_test")


@pipeline.node
def my_node() -> pl.DataFrame:
    """This is the description."""
    return pl.DataFrame()
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        assert graph["nodes"][0]["data"]["description"] == "This is the description."

    def test_empty_file_returns_empty_graph(self, tmp_path):
        p = _write_pipeline(tmp_path, "")
        graph = parse_pipeline_file(p)
        assert graph["nodes"] == []

    def test_preamble_extracted(self, tmp_path):
        code = '''\
import polars as pl
import haute

from pathlib import Path

DATA_DIR = Path("data")

pipeline = haute.Pipeline("preamble_test")


@pipeline.node
def src() -> pl.DataFrame:
    return pl.DataFrame()
'''
        p = _write_pipeline(tmp_path, code)
        graph = parse_pipeline_file(p)
        preamble = graph.get("preamble", "")
        assert "DATA_DIR" in preamble


class TestParsePipelineRoundtrip:
    """Test that parse → codegen → parse produces consistent results."""

    def test_roundtrip_preserves_structure(self, tmp_path):
        from haute.codegen import graph_to_code

        code = '''\
import polars as pl
import haute

pipeline = haute.Pipeline("roundtrip")


@pipeline.node(path="data.parquet")
def source() -> pl.DataFrame:
    """Load data."""
    return pl.scan_parquet("data.parquet")


@pipeline.node
def transform(source: pl.DataFrame) -> pl.DataFrame:
    """Transform."""
    return source


pipeline.connect("source", "transform")
'''
        p = _write_pipeline(tmp_path, code)
        graph1 = parse_pipeline_file(p)

        generated = graph_to_code(graph1, pipeline_name="roundtrip")
        p2 = tmp_path / "roundtrip2.py"
        p2.write_text(generated)
        graph2 = parse_pipeline_file(p2)

        assert len(graph1["nodes"]) == len(graph2["nodes"])
        assert len(graph1["edges"]) == len(graph2["edges"])

        names1 = {n["id"] for n in graph1["nodes"]}
        names2 = {n["id"] for n in graph2["nodes"]}
        assert names1 == names2
