# Execution Trace & Data Lineage - Design

## Problem

A pricing pipeline transforms input features through N nodes into an output price.
For regulatory explainability (Solvency II, IFRS 17) and day-to-day debugging,
users need to answer: **"Why did this policy get this price?"**

The current `execute_graph` already captures per-node DataFrames. The trace system
extends this to provide **row-level, column-level, and cell-level lineage** through
the full DAG.

### Scale assumptions

A mature motor pricing pipeline might have:

- **50–100 nodes** (data sources, feature engineering, model scores, rating steps, blenders, outputs)
- **100–300 columns** flowing through the graph (raw features, derived features, model scores, factors, intermediate prices)
- **Branching/merging DAGs** - separate frequency/severity branches that merge at a blender node
- **Cardinality-changing operations** - joins (fan-out), group_bys (collapse), filters (reduce)
- **Multiple model scores** - GLMs, GBMs, blended outputs

The trace system must handle all of this without requiring O(nodes × rows × columns) memory.

---

## Three levels of lineage

### Level 1 - Node lineage (static, free)

> "Which nodes contributed to this output?"

Already available via `graph_utils.ancestors()`. Given any node, walk backwards
through edges to find all upstream nodes. This is the **path highlight** on the
React Flow graph - no execution required.

**Augmentation for column lineage**: At each node, diff the input schema against
the output schema to classify every column as `added`, `removed`, `passed_through`,
or `modified`. This gives a **column-level node lineage** - when tracing a specific
column, we can dim nodes that don't touch it.

```
Schema diff at node "area_factor":
  input:  [postcode, driver_age, vehicle_age, base_rate]
  output: [postcode, driver_age, vehicle_age, base_rate, area_factor]
  → added: [area_factor]
  → passed_through: [postcode, driver_age, vehicle_age, base_rate]
```

### Level 2 - Row trace (dynamic, cheap for 1 row)

> "For this specific policy, what happened at each node?"

Execute the pipeline on a **single row** (or small subset) and capture the full
DataFrame snapshot at every node. Since we're processing 1 row through Polars
lazy evaluation, this is near-instant even with 100 nodes.

This is the "click a row → light up the graph with intermediate values" feature.

### Level 3 - Cell trace (dynamic, targeted)

> "Why is this specific cell £412.50?"

Given an output (row_index, column_name), trace backwards through the graph
showing only the **relevant columns** at each node. This requires column
provenance - knowing which upstream columns a given output column depends on.

This is the deepest level: the `base rate £300 → area factor ×1.2 → NCD ×0.85 → £412.50` view.

---

## Data structures

### TraceRequest

```python
@dataclass
class TraceRequest:
    graph: dict                    # React Flow graph JSON
    row_index: int                 # which output row to trace (0-indexed)
    target_node_id: str            # which node's output we're starting from
    column: str | None = None      # optional: specific column to trace
    input_overrides: dict | None = None  # optional: override input values (for what-if)
```

### TraceStep

```python
@dataclass
class TraceStep:
    node_id: str
    node_name: str
    node_type: str                 # dataSource, transform, modelScore, ratingStep, output

    # Schema changes at this node
    columns_added: list[str]
    columns_removed: list[str]
    columns_modified: list[str]
    columns_passed: list[str]

    # Row snapshot - only the relevant columns (all columns if no column filter)
    input_values: dict[str, Any]   # column → value, before this node
    output_values: dict[str, Any]  # column → value, after this node

    # Human-readable explanation (optional, generated for known node types)
    expression: str | None = None

    # For join nodes: which row(s) from each branch matched
    join_info: JoinInfo | None = None

    # For group_by nodes: how many source rows collapsed into this one
    aggregation_info: AggregationInfo | None = None

    # Timing
    execution_ms: float = 0.0
```

### JoinInfo

```python
@dataclass
class JoinInfo:
    join_keys: dict[str, Any]          # key columns and their values
    left_branch_node: str              # which upstream node provided the left side
    right_branch_node: str             # which upstream node provided the right side
    left_row_count: int                # how many left rows matched
    right_row_count: int               # how many right rows matched
```

### AggregationInfo

```python
@dataclass
class AggregationInfo:
    group_keys: dict[str, Any]         # group-by columns and their values
    source_row_count: int              # how many rows collapsed into this group
    aggregation_type: str              # "group_by", "sum", "mean", etc.
```

### TraceResult

```python
@dataclass
class TraceResult:
    target_node_id: str
    row_index: int
    column: str | None
    output_value: Any                  # the value being explained

    # Ordered path through the DAG (may branch for joins)
    steps: list[TraceStep]

    # For branching DAGs: parallel paths that converge at join nodes
    branches: dict[str, list[TraceStep]]  # branch_name → steps

    # Human-readable summary
    summary: str                       # "base £300 × area 1.2 × NCD 0.85 = £412.50"

    # Metadata
    total_nodes_in_pipeline: int
    nodes_in_trace: int                # how many nodes are relevant to this trace
    execution_ms: float
```

---

## Execution model

### Instrumented executor

A new function `execute_trace` wraps the existing `execute_graph` logic but
instruments it to capture snapshots:

```
execute_trace(graph, row_index, target_node_id, column?)
    │
    ├── 1. Run full pipeline (or up to target_node) → per-node LazyFrames
    │      (reuse execute_graph internals, keep lazy_outputs dict)
    │
    ├── 2. Identify the target output row
    │      collect target node, pick row at row_index
    │
    ├── 3. Walk backwards through DAG from target node
    │      At each node, find the corresponding row(s) in its output
    │      (using join keys / row identity to track through joins)
    │
    ├── 4. At each node, compute schema diff (input vs output columns)
    │
    ├── 5. If column filter: prune to only nodes that touch that column
    │
    └── 6. Build TraceResult with steps, branches, summary
```

### Row identity tracking

The hardest part: **tracking which output row corresponds to which input row(s)
across nodes that change cardinality.**

Strategy by node type:

| Node type | Cardinality | Row tracking strategy |
|-----------|-------------|----------------------|
| **Transform** (map) | 1:1 | Same row index. Input row N → output row N. |
| **Filter** | N:M (M≤N) | Track via a hidden `__trace_row_id` column injected before execution. |
| **Join** | N:M (fan-out/reduce) | Track via join key values. Find matching rows in both branches. |
| **Group_by** | N:M (M≤N) | Track via group key values. Record "this group had K source rows". |
| **Sort** | 1:1 (reorder) | Track via `__trace_row_id`. |
| **Concat/Union** | N+M | Track via `__trace_row_id` + branch tag. |

**Implementation**: Before executing the trace, inject a `__trace_row_id` column
(monotonic integer) into every source node's output. This survives through maps,
filters, and sorts. For joins and group_bys, use the key columns to trace back.

```python
# Injected at source nodes during trace execution
lf = lf.with_row_index("__trace_row_id")
```

After trace collection, strip `__trace_row_id` from all user-visible output.

### Column provenance

For cell-level tracing, we need to know: **which input columns does output column X
depend on?**

Three approaches, used in combination:

1. **Schema diff heuristic** (cheap, good enough for 80% of cases):
   - If a column exists in input and output with the same name and same values → `passed_through`
   - If a column exists only in output → `added` (depends on... we don't know yet)
   - If a column's values changed → `modified`

2. **Polars expression plan inspection** (medium effort, high accuracy):
   Polars `LazyFrame` has a query plan accessible via `lf.explain()`. For nodes
   with user code, we can inspect the `with_columns` / `select` expressions to
   determine column dependencies. E.g., `pl.col("base_rate") * pl.col("area_factor")`
   depends on `["base_rate", "area_factor"]`.

   ```python
   # Future: parse Polars expression trees for column deps
   def column_deps(expr: pl.Expr) -> set[str]:
       """Extract column names referenced by a Polars expression."""
       plan_str = pl.LazyFrame().select(expr).explain()
       # parse column references from plan string
       ...
   ```

3. **User annotation** (explicit, for complex/opaque nodes):
   For model scoring nodes and rating step nodes, the user (or the framework)
   can declare column dependencies:

   ```python
   @pipeline.model_score(
       inputs=["driver_age", "vehicle_age", "postcode"],
       outputs=["predicted_frequency"],
   )
   def score_frequency(df: pl.DataFrame) -> pl.DataFrame:
       ...
   ```

   This annotation is optional - the trace system works without it but gives
   better targeted traces when present.

---

## Handling complex DAG patterns

### Pattern 1: Linear chain

```
Source → Transform A → Transform B → Output
```

Trace is a simple list of steps. Each step shows input/output values.

### Pattern 2: Fan-out / fan-in (branch and merge)

```
Source → Feature Eng → Frequency Model ──┐
                    → Severity Model  ───┤
                                         ├→ Blender → Output
```

The trace **branches** at the fan-out point. The `TraceResult.branches` dict
holds parallel paths. At the Blender node, the trace shows values arriving
from both branches and how they're combined.

Frontend rendering: show two parallel highlighted paths on the graph, converging
at the Blender node. The side panel shows both branches with their values.

### Pattern 3: Multi-source join

```
Policy Data ──────→ Join ──→ Rating Step → Output
Area Rating Table ─→ Join
```

The trace shows the **join keys** and which row from each source was matched.
The `JoinInfo` on the trace step records:
- Join keys: `{postcode: "SW1A"}`
- Left: policy row with 15 columns
- Right: area table row with `{postcode: "SW1A", area_factor: 1.2}`

### Pattern 4: Sub-pipeline (Phase 6)

When composable pipelines are introduced, a sub-pipeline appears as a single
node in the parent trace. The user can "drill in" to expand it into its own
trace steps. This is just a recursive `TraceResult` nested inside a `TraceStep`.

### Pattern 5: Aggregation then join-back

```
Source → Group_by(region) → Avg claim cost per region ──→ Join back → Output
       └────────────────────────────────────────────────→ Join back
```

The group_by step shows: "This output row (region=East) was derived from 1,247
source rows". The join-back step shows the aggregated value being attached to
the original row.

---

## API endpoints

### POST /api/pipeline/trace

Full row trace through the pipeline.

```json
// Request
{
    "graph": { "nodes": [...], "edges": [...] },
    "rowIndex": 42,
    "targetNodeId": "output",
    "column": "premium"       // optional
}

// Response
{
    "status": "ok",
    "trace": {
        "target_node_id": "output",
        "row_index": 42,
        "column": "premium",
        "output_value": 412.50,
        "steps": [
            {
                "node_id": "read_data",
                "node_name": "Read Data",
                "node_type": "dataSource",
                "columns_added": ["postcode", "driver_age", "vehicle_age", "..."],
                "columns_removed": [],
                "columns_modified": [],
                "input_values": {},
                "output_values": {
                    "postcode": "SW1A",
                    "driver_age": 35,
                    "vehicle_age": 3,
                    "base_rate": 300.0
                },
                "expression": "Source: data/policies.parquet (row 42)"
            },
            {
                "node_id": "area_factor",
                "node_name": "Area Factor",
                "node_type": "ratingStep",
                "columns_added": ["area_factor"],
                "columns_removed": [],
                "columns_modified": [],
                "input_values": { "postcode": "SW1A", "base_rate": 300.0 },
                "output_values": { "postcode": "SW1A", "base_rate": 300.0, "area_factor": 1.2 },
                "expression": "Lookup area_table on postcode='SW1A' → area_factor=1.2"
            },
            {
                "node_id": "apply_ncd",
                "node_name": "Apply NCD",
                "node_type": "ratingStep",
                "columns_added": ["ncd_factor"],
                "columns_removed": [],
                "columns_modified": [],
                "input_values": { "ncd_years": 5 },
                "output_values": { "ncd_years": 5, "ncd_factor": 0.85 },
                "expression": "Lookup ncd_table on ncd_years=5 → ncd_factor=0.85"
            },
            {
                "node_id": "output",
                "node_name": "Technical Price",
                "node_type": "output",
                "columns_added": ["premium"],
                "columns_removed": ["...intermediate cols..."],
                "columns_modified": [],
                "input_values": { "base_rate": 300.0, "area_factor": 1.2, "ncd_factor": 0.85, "freq_load": 1.35 },
                "output_values": { "premium": 412.50 },
                "expression": "base_rate × area_factor × ncd_factor × freq_load = 412.50"
            }
        ],
        "branches": {},
        "summary": "base_rate 300.0 × area_factor 1.2 × ncd_factor 0.85 × freq_load 1.35 = 412.50",
        "total_nodes_in_pipeline": 12,
        "nodes_in_trace": 4,
        "execution_ms": 23.4
    }
}
```

### POST /api/pipeline/trace/column

Column lineage across the entire pipeline (not row-specific).

```json
// Request
{
    "graph": { "nodes": [...], "edges": [...] },
    "column": "premium"
}

// Response
{
    "column": "premium",
    "lineage": [
        {
            "node_id": "output",
            "action": "added",
            "depends_on": ["base_rate", "area_factor", "ncd_factor", "freq_load"]
        },
        {
            "node_id": "area_factor",
            "action": "added",
            "column": "area_factor",
            "depends_on": ["postcode"]
        }
    ],
    "origin_columns": ["postcode", "ncd_years", "base_rate", "predicted_freq"],
    "relevant_nodes": ["read_data", "area_factor", "apply_ncd", "score_freq", "output"],
    "irrelevant_nodes": ["clean_vehicle", "driver_age_band", "..."]
}
```

### POST /api/pipeline/trace/compare

Compare traces for two rows side-by-side (useful for "why does policy A cost more than B?").

```json
// Request
{
    "graph": { ... },
    "rowIndexA": 42,
    "rowIndexB": 99,
    "targetNodeId": "output",
    "column": "premium"
}

// Response
{
    "comparison": {
        "row_a": { "premium": 412.50, "steps": [...] },
        "row_b": { "premium": 287.30, "steps": [...] },
        "differences": [
            {
                "node_id": "area_factor",
                "column": "area_factor",
                "value_a": 1.2,
                "value_b": 0.9,
                "impact": "+33%"
            },
            {
                "node_id": "apply_ncd",
                "column": "ncd_factor",
                "value_a": 0.85,
                "value_b": 0.70,
                "impact": "+21%"
            }
        ]
    }
}
```

---

## Performance & scaling strategy

### Cost model

| Operation | Cost | When |
|-----------|------|------|
| Node lineage (Level 1) | O(edges) graph walk | Static, instant |
| Schema diff per node | O(columns) per node | Once per execution, cached |
| Row trace (Level 2) | O(nodes) × 1-row Polars execution | Per click, ~10–50ms for 100 nodes |
| Cell trace (Level 3) | Same as row trace + column pruning | Per click |
| Compare trace | 2× row trace | Per click |

### Caching strategy

```
TraceCache:
    graph_hash → {
        schema_diffs: dict[node_id, SchemaDiff]      # recompute on graph change
        last_execution: dict[node_id, LazyFrame]      # reuse for multiple trace clicks
        row_traces: LRU[row_index → TraceResult]      # cache recent row traces
    }
```

- **Schema diffs**: Computed once per pipeline execution, cached until the graph changes.
- **LazyFrame intermediates**: Kept from the last `execute_graph` call. When the user clicks different rows, we don't re-execute the pipeline - we just collect different rows from the cached lazy frames.
- **Row traces**: LRU cache of recent trace results. Pricing analysts typically trace 5–10 rows when investigating a pricing issue.

### Large pipeline optimisation

For pipelines with 50+ nodes:

1. **Lazy collection**: Don't `.collect()` intermediate nodes until needed for the trace. Keep everything lazy. Only collect the target node + the specific row, then walk backwards collecting one row at a time.

2. **Column pruning on trace**: When tracing a specific column, use column lineage to skip nodes that don't affect it. In a 100-node pipeline, the trace for a single column might only touch 15 nodes.

3. **Parallel branch execution**: When the DAG branches, trace each branch independently (can be parallelised with `asyncio.gather`).

4. **Streaming for batch traces**: For regulatory reports that need traces for every row, use Polars streaming execution with a custom sink that captures per-node snapshots.

### Memory budget

For a single-row trace through a 100-node pipeline with 200 columns:
- Each snapshot ≈ 200 values × ~50 bytes = ~10KB
- 100 nodes × 10KB = ~1MB per trace
- Cache 100 traces = ~100MB

This is well within budget for a local dev server.

---

## Implementation plan

### Phase A - Instrumented single-row executor (`src/haute/trace.py`)

New module that wraps `execute_graph` logic:

1. Accept a `TraceRequest`
2. Execute the full pipeline lazily (reuse `_build_node_fn` from executor)
3. At each node, capture input/output schemas
4. Collect the target row from each node's output
5. Build `TraceStep` objects with schema diffs and value snapshots
6. Return `TraceResult`

**Depends on**: existing executor infrastructure (no changes needed to `execute_graph`)

### Phase B - Schema diff engine

Function that compares two DataFrames (input vs output of a node) and classifies
each column:

```python
def schema_diff(input_df: pl.DataFrame, output_df: pl.DataFrame) -> SchemaDiff:
    input_cols = set(input_df.columns)
    output_cols = set(output_df.columns)
    
    added = output_cols - input_cols
    removed = input_cols - output_cols
    common = input_cols & output_cols
    
    # For common columns, check if values changed (for the traced row)
    modified = set()
    passed = set()
    for col in common:
        if input_df[col].item() != output_df[col].item():
            modified.add(col)
        else:
            passed.add(col)
    
    return SchemaDiff(added, removed, modified, passed)
```

### Phase C - Row identity tracking

Inject `__trace_row_id` at source nodes. Track through the DAG:

- **1:1 transforms**: row_id preserved automatically
- **Filters**: row_id preserved (some rows filtered out)
- **Joins**: match on join keys, record both source row_ids
- **Group_bys**: record group keys and count of source rows

### Phase D - API endpoint + frontend integration

1. `POST /api/pipeline/trace` endpoint in `server.py`
2. Frontend: click a cell in the data preview → call trace API → highlight path on graph → show trace panel

### Phase E - Column lineage (static analysis)

Parse node code (Polars expressions) to determine column dependencies.
This enables the "dim irrelevant nodes" feature for column-specific traces.

### Phase F - Expression generation

For known node types (rating steps, simple transforms), generate human-readable
expression strings:

- Rating step: `"Lookup area_table on postcode='SW1A' → area_factor=1.2"`
- Multiply: `"base_rate × area_factor × ncd_factor = 412.50"`
- Filter: `"Kept: driver_age >= 18 (True)"`

### Phase G - Compare traces + What-If integration

- Compare two rows side-by-side with diff highlighting
- Connect to What-If mode: when a slider changes an input, re-run the trace
  and animate the value changes propagating through the graph

---

## File structure

```
src/haute/
├── trace.py              # TraceRequest, TraceResult, execute_trace()
├── schema_diff.py        # SchemaDiff, column classification
├── column_lineage.py     # Static column provenance analysis
├── expression_gen.py     # Human-readable expression generation
├── executor.py           # (existing) - add trace-aware hooks
├── server.py             # (existing) - add /api/pipeline/trace endpoints
```

---

## Frontend UX (brief notes for later)

### Trace panel (right sidebar)

When a user clicks a cell in the data preview:

1. The path through the graph lights up (edges glow, relevant nodes highlighted)
2. Irrelevant nodes dim to 30% opacity
3. Each relevant node shows a small badge with the intermediate value
4. A "Trace" panel slides in from the right showing the step-by-step breakdown
5. The summary line ("base £300 × area 1.2 × ...") appears at the top

### For large graphs

- The minimap highlights the trace path
- Nodes outside the trace path collapse into a "... 12 nodes ..." placeholder
- Clicking the placeholder expands them (dimmed)
- Branching traces show parallel columns in the trace panel

### Compare mode

- Split the trace panel into two columns (Row A | Row B)
- Highlight cells where values differ between the two rows
- Show percentage impact of each difference

---

## Interaction with What-If mode (Section 9.2)

The trace and what-if features share infrastructure:

- **What-If** = re-execute pipeline on 1 modified row, show per-node results
- **Trace** = execute pipeline on 1 row, show per-node results with lineage metadata

The only difference is that What-If varies inputs via sliders, while Trace
explains a specific output. They use the same single-row execution path and
can share the same cached intermediates.

When combined: drag a slider → the trace updates live, showing how the value
change propagates through each node. This is the ultimate "demo moment".

---

## Relationship to regulatory requirements

### Solvency II (Article 45, ORSA)

Requires insurers to demonstrate understanding of their risk profile, including
how pricing decisions are made. The trace provides per-policy explainability.

### IFRS 17 (Insurance Contracts)

Requires detailed breakdown of premium components for financial reporting.
The trace's step-by-step value decomposition maps directly to this.

### FCA Consumer Duty (UK)

Requires firms to demonstrate that pricing outcomes are fair. The compare-trace
feature ("why does policy A cost more than B?") directly supports fair value
assessments.

### Deliverable

A `haute trace export` CLI command that generates a regulatory-ready PDF/HTML
report showing the full trace for a set of policies. This is a future CLI
command but the backend infrastructure (trace data structures) is designed
to support it from day one.
