"""End-to-end integration test: parse → execute → trace → codegen → re-parse.

Exercises the full haute pipeline lifecycle using the fixture pipeline to
verify all stages interoperate correctly.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from haute.codegen import graph_to_code
from haute.executor import execute_graph
from haute.parser import parse_pipeline_file, parse_pipeline_source
from haute.trace import execute_trace

FIXTURE_DIR = Path("tests/fixtures")
PIPELINE_FILE = FIXTURE_DIR / "pipeline.py"


class TestEndToEnd:
    """Full round-trip: parse → execute → trace → codegen → re-parse."""

    def test_parse_fixture_pipeline(self):
        """Fixture pipeline parses into a valid graph."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        assert len(graph.nodes) >= 5
        assert len(graph.edges) >= 5
        node_ids = {n.id for n in graph.nodes}
        assert "quotes" in node_ids
        assert "output" in node_ids

    def test_execute_all_nodes(self):
        """All nodes execute successfully with status='ok'."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        results = execute_graph(graph)
        for nid, result in results.items():
            assert result.status == "ok", (
                f"Node {nid!r} failed: {result.error}"
            )
            assert result.row_count > 0

    def test_execute_target_node(self):
        """Executing with a target returns results for the target and its ancestors."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        results = execute_graph(graph, target_node_id="output")
        assert "output" in results
        assert results["output"].status == "ok"
        # All ancestors of output should be present
        assert "calculate_premium" in results
        assert "area_lookup" in results

    def test_trace_produces_steps(self):
        """Trace returns steps for every node in the execution chain."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        trace = execute_trace(graph, row_index=0, target_node_id="output")
        assert len(trace.steps) > 0
        step_names = [s.node_id for s in trace.steps]
        assert "output" in step_names

    def test_trace_row_has_data(self):
        """Each trace step includes output values and schema diff."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        trace = execute_trace(graph, row_index=0, target_node_id="output")
        for step in trace.steps:
            assert isinstance(step.output_values, dict)
            assert step.schema_diff is not None

    def test_codegen_produces_code(self):
        """Code generated from the parsed graph is non-empty and valid Python text."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        code = graph_to_code(graph, pipeline_name="roundtrip")
        assert len(code) > 100
        assert "Pipeline" in code
        assert "pipeline.node" in code or "@pipeline.node" in code

    def test_codegen_roundtrip_preserves_structure(self, tmp_path):
        """parse → codegen → re-parse preserves node types."""
        from haute._config_io import collect_node_configs

        graph = parse_pipeline_file(PIPELINE_FILE)
        code = graph_to_code(graph, pipeline_name="roundtrip")

        # Write config files so the parser can resolve them
        for rel_path, content in collect_node_configs(graph).items():
            cfg_file = tmp_path / rel_path
            cfg_file.parent.mkdir(parents=True, exist_ok=True)
            cfg_file.write_text(content)

        reparsed = parse_pipeline_source(code, _base_dir=tmp_path)

        orig_types = sorted(n.data.nodeType for n in graph.nodes)
        new_types = sorted(n.data.nodeType for n in reparsed.nodes)
        assert orig_types == new_types

    def test_codegen_roundtrip_preserves_node_count(self):
        """Re-parsed graph has the same number of nodes."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        code = graph_to_code(graph, pipeline_name="roundtrip")
        reparsed = parse_pipeline_source(code)
        assert len(reparsed.nodes) == len(graph.nodes)

    def test_codegen_roundtrip_preserves_edge_count(self):
        """Re-parsed graph has the same number of edges."""
        graph = parse_pipeline_file(PIPELINE_FILE)
        code = graph_to_code(graph, pipeline_name="roundtrip")
        reparsed = parse_pipeline_source(code)
        assert len(reparsed.edges) == len(graph.edges)

    def test_full_lifecycle(self, tmp_path):
        """Complete lifecycle: parse → execute → trace → codegen → re-parse → re-execute."""
        from haute._config_io import collect_node_configs

        # Step 1: Parse
        graph = parse_pipeline_file(PIPELINE_FILE)
        assert len(graph.nodes) > 0

        # Step 2: Execute
        results = execute_graph(graph)
        output_result = results["output"]
        assert output_result.status == "ok"
        orig_row_count = output_result.row_count
        orig_columns = sorted(c.name for c in output_result.columns)

        # Step 3: Trace
        trace = execute_trace(graph, row_index=0, target_node_id="output")
        assert len(trace.steps) > 0

        # Step 4: Codegen → re-parse
        code = graph_to_code(graph, pipeline_name="lifecycle")

        # Write config files so the parser can resolve them
        for rel_path, content in collect_node_configs(graph).items():
            cfg_file = tmp_path / rel_path
            cfg_file.parent.mkdir(parents=True, exist_ok=True)
            cfg_file.write_text(content)

        reparsed = parse_pipeline_source(code, _base_dir=tmp_path)
        assert len(reparsed.nodes) == len(graph.nodes)

        # Step 5: Re-execute the re-parsed graph
        results2 = execute_graph(reparsed)
        output_result2 = results2["output"]
        assert output_result2.status == "ok"
        assert output_result2.row_count == orig_row_count
        # Core input columns must survive the round-trip
        reparsed_columns = {c.name for c in output_result2.columns}
        assert "VehPower" in reparsed_columns
        assert "Area" in reparsed_columns
        assert "Exposure" in reparsed_columns
