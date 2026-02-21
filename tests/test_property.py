"""Property-based tests using Hypothesis.

These tests verify invariants that must hold for *all* inputs, not just
hand-picked examples.
"""

from __future__ import annotations

import string
from pathlib import Path

import polars as pl
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from haute._types import GraphEdge, GraphNode, NodeData, NodeType, PipelineGraph
from haute.codegen import graph_to_code
from haute.executor import _apply_banding
from haute.graph_utils import _sanitize_func_name, topo_sort_ids
from haute.parser import parse_pipeline_file
from tests.conftest import make_edge

# ---------------------------------------------------------------------------
# _sanitize_func_name properties
# ---------------------------------------------------------------------------

# Strategy: arbitrary strings with printable chars, spaces, hyphens, digits
label_strategy = st.text(
    alphabet=string.printable,
    min_size=0,
    max_size=50,
)


class TestSanitizeFuncNameProperties:
    @given(label=label_strategy)
    @settings(max_examples=200)
    def test_always_returns_valid_python_identifier(self, label: str):
        """For any input string, the result is a valid Python identifier."""
        result = _sanitize_func_name(label)
        assert result.isidentifier(), (
            f"_sanitize_func_name({label!r}) = {result!r} is not a valid identifier"
        )

    @given(label=label_strategy)
    @settings(max_examples=200)
    def test_never_returns_empty_string(self, label: str):
        """Output is never empty — falls back to 'unnamed_node'."""
        result = _sanitize_func_name(label)
        assert len(result) > 0

    @given(label=label_strategy)
    @settings(max_examples=200)
    def test_is_deterministic(self, label: str):
        """Same input always produces same output."""
        assert _sanitize_func_name(label) == _sanitize_func_name(label)

    @given(label=st.text(alphabet=string.ascii_letters + "_", min_size=1, max_size=20))
    @settings(max_examples=100)
    def test_preserves_valid_identifiers(self, label: str):
        """Input that is already a valid identifier is preserved (mostly)."""
        assume(label.isidentifier())
        result = _sanitize_func_name(label)
        assert result == label


# ---------------------------------------------------------------------------
# topo_sort_ids properties
# ---------------------------------------------------------------------------

def dag_strategy():
    """Generate a random DAG as (node_ids, edges).

    Creates N nodes and a random subset of forward edges (i→j where i<j)
    to ensure acyclicity.
    """
    @st.composite
    def make_dag(draw):
        n = draw(st.integers(min_value=0, max_value=8))
        ids = [f"n{i}" for i in range(n)]
        edges = []
        for i in range(n):
            for j in range(i + 1, n):
                if draw(st.booleans()):
                    edges.append(make_edge(ids[i], ids[j]))
        return ids, edges
    return make_dag()


class TestTopoSortProperties:
    @given(data=dag_strategy())
    @settings(max_examples=200)
    def test_topological_invariant(self, data):
        """Every edge (u→v) must have u before v in the sorted output."""
        ids, edges = data
        result = topo_sort_ids(ids, edges)
        idx = {nid: i for i, nid in enumerate(result)}
        for e in edges:
            if e.source in idx and e.target in idx:
                assert idx[e.source] < idx[e.target], (
                    f"{e.source} should come before {e.target}"
                )

    @given(data=dag_strategy())
    @settings(max_examples=200)
    def test_preserves_all_nodes(self, data):
        """All input nodes should appear in the output (for a DAG, no cycles)."""
        ids, edges = data
        result = topo_sort_ids(ids, edges)
        assert set(result) == set(ids)

    @given(data=dag_strategy())
    @settings(max_examples=200)
    def test_is_deterministic(self, data):
        """Same DAG always produces same ordering."""
        ids, edges = data
        assert topo_sort_ids(ids, edges) == topo_sort_ids(ids, edges)


# ---------------------------------------------------------------------------
# _apply_banding properties
# ---------------------------------------------------------------------------

class TestBandingProperties:
    @given(
        values=st.lists(
            st.floats(allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6),
            min_size=1, max_size=50,
        ),
        threshold=st.floats(
            allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6,
        ),
    )
    @settings(max_examples=100)
    def test_continuous_banding_covers_all_rows(self, values, threshold):
        """Two complementary rules (<=t, >t) should assign every row."""
        lf = pl.DataFrame({"x": values}).lazy()
        rules = [
            {"op1": "<=", "val1": threshold, "assignment": "low"},
            {"op1": ">", "val1": threshold, "assignment": "high"},
        ]
        result = _apply_banding(lf, "x", "band", "continuous", rules).collect()
        assert result["band"].null_count() == 0
        assert set(result["band"].to_list()) <= {"low", "high"}

    @given(
        values=st.lists(st.sampled_from(["A", "B", "C", "D", "E"]), min_size=1, max_size=30),
    )
    @settings(max_examples=100)
    def test_categorical_banding_matches_rules(self, values):
        """Categorical banding only produces assignments from the rules + None."""
        lf = pl.DataFrame({"x": values}).lazy()
        rules = [
            {"value": "A", "assignment": "Group1"},
            {"value": "B", "assignment": "Group1"},
            {"value": "C", "assignment": "Group2"},
        ]
        result = _apply_banding(lf, "x", "band", "categorical", rules).collect()
        valid_outputs = {"Group1", "Group2", None}
        actual = set(result["band"].to_list())
        assert actual <= valid_outputs

    @given(
        values=st.lists(
            st.floats(allow_nan=False, allow_infinity=False, min_value=-1e6, max_value=1e6),
            min_size=1, max_size=20,
        ),
    )
    @settings(max_examples=100)
    def test_banding_preserves_row_count(self, values):
        """Banding never changes the number of rows."""
        lf = pl.DataFrame({"x": values}).lazy()
        rules = [{"op1": "<=", "val1": 0, "assignment": "neg"}]
        result = _apply_banding(lf, "x", "band", "continuous", rules).collect()
        assert len(result) == len(values)


# ---------------------------------------------------------------------------
# Codegen ↔ parser roundtrip properties
# ---------------------------------------------------------------------------


def _pipeline_graph_strategy():
    """Generate a random DAG-based pipeline graph.

    Builds a chain: dataSource → N transforms → output.
    Uses valid Python identifiers as labels to survive codegen roundtrip.
    """
    @st.composite
    def make_pipeline(draw):
        # 0..4 intermediate transform nodes
        n_transforms = draw(st.integers(min_value=0, max_value=4))

        # Build unique labels — codegen uses these as Python function names
        all_labels: list[str] = []
        all_labels.append("Source")

        for i in range(n_transforms):
            label = f"Step{i}"
            all_labels.append(label)

        all_labels.append("Output")

        nodes: list[GraphNode] = []
        edges: list[GraphEdge] = []

        # Source node
        nodes.append(GraphNode(
            id="source",
            data=NodeData(
                label="Source",
                nodeType=NodeType.DATA_SOURCE,
                config={"path": "data/input.parquet"},
            ),
        ))

        # Transform nodes
        for i in range(n_transforms):
            nid = f"step{i}"
            nodes.append(GraphNode(
                id=nid,
                data=NodeData(
                    label=f"Step{i}",
                    nodeType=NodeType.TRANSFORM,
                    config={"code": ""},
                ),
            ))

        # Output node
        nodes.append(GraphNode(
            id="output",
            data=NodeData(
                label="Output",
                nodeType=NodeType.OUTPUT,
                config={"fields": []},
            ),
        ))

        # Chain edges: source → step0 → step1 → ... → output
        prev_id = "source"
        for i in range(n_transforms):
            nid = f"step{i}"
            edges.append(GraphEdge(id=f"e_{prev_id}_{nid}", source=prev_id, target=nid))
            prev_id = nid
        edges.append(GraphEdge(id=f"e_{prev_id}_output", source=prev_id, target="output"))

        return PipelineGraph(
            nodes=nodes,
            edges=edges,
            pipeline_name="gen_pipeline",
        )

    return make_pipeline()


def _roundtrip(graph: PipelineGraph) -> PipelineGraph:
    """Helper: codegen → write to temp file → parse back."""
    import tempfile

    code = graph_to_code(graph, pipeline_name="gen_pipeline")
    td = Path(tempfile.mkdtemp())
    py_file = td / "gen_pipeline.py"
    py_file.write_text(code)
    return parse_pipeline_file(py_file)


class TestCodegenRoundtripProperties:
    @given(graph=_pipeline_graph_strategy())
    @settings(max_examples=50, deadline=5000)
    def test_roundtrip_preserves_node_count(self, graph: PipelineGraph):
        """codegen → parse should preserve the number of nodes."""
        parsed = _roundtrip(graph)
        assert len(parsed.nodes) == len(graph.nodes)

    @given(graph=_pipeline_graph_strategy())
    @settings(max_examples=50, deadline=5000)
    def test_roundtrip_preserves_edge_count(self, graph: PipelineGraph):
        """codegen → parse should preserve the number of edges."""
        parsed = _roundtrip(graph)
        assert len(parsed.edges) == len(graph.edges)

    @given(graph=_pipeline_graph_strategy())
    @settings(max_examples=50, deadline=5000)
    def test_roundtrip_preserves_node_labels(self, graph: PipelineGraph):
        """codegen → parse should preserve node labels (as function names)."""
        parsed = _roundtrip(graph)
        original_labels = {n.data.label for n in graph.nodes}
        parsed_labels = {n.data.label for n in parsed.nodes}
        assert original_labels == parsed_labels

    @given(graph=_pipeline_graph_strategy())
    @settings(max_examples=50, deadline=5000)
    def test_roundtrip_code_compiles(self, graph: PipelineGraph):
        """Generated code should always be valid Python."""
        code = graph_to_code(graph, pipeline_name="gen_pipeline")
        compile(code, "<generated>", "exec")

    @given(graph=_pipeline_graph_strategy())
    @settings(max_examples=50, deadline=5000)
    def test_roundtrip_preserves_pipeline_name(self, graph: PipelineGraph):
        """codegen → parse should preserve the pipeline name."""
        parsed = _roundtrip(graph)
        assert parsed.pipeline_name == "gen_pipeline"
