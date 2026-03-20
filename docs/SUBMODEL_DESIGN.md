# Submodels — Design Document

## 1. Overview

Submodels let users **select a group of nodes** in the GUI and **collapse them into a single reusable unit**. In code, the grouped nodes are extracted to a separate `.py` file that the main pipeline imports. Users can click into a submodel to see and edit its internal nodes.

**Core principle**: submodels are a **code-organisation and GUI-navigation** concept, not an execution concept. The executor, trace engine, and deploy pipeline always work on a **flat graph** — they never see submodel boundaries. This keeps changes minimal in the most complex parts of the codebase.

---

## 2. User Experience

### 2.1 Creating a Submodel (GUI)

1. User selects multiple nodes (rubber-band or Ctrl+click)
2. Right-click → **"Group as Submodel"** (or keyboard shortcut Ctrl+G)
3. Dialog appears: enter submodel name (e.g. `model_scoring`)
4. The selected nodes collapse into a single **submodel node** on the canvas
5. The submodel node shows:
   - A distinct visual style (folder/box icon, accent colour)
   - Input ports (one per incoming external edge)
   - Output ports (one per outgoing external edge)
   - A small count badge: "3 nodes"
6. A new file `modules/<name>.py` is created on disk
7. `main.py` is updated to replace the inline node definitions with a `pipeline.submodel()` import

### 2.2 Navigating Into a Submodel

1. **Double-click** the submodel node → canvas transitions to show only the submodel's internal nodes
2. A **breadcrumb bar** appears: `my_pipeline > model_scoring`
3. Input/output boundary nodes appear as special **port nodes** pinned to the left/right edges
4. User can edit nodes normally inside the submodel view
5. Click the breadcrumb or press **Escape** to navigate back to the parent

### 2.3 Dissolving a Submodel

1. Right-click a submodel node → **"Ungroup Submodel"**
2. The internal nodes are inlined back into the parent graph
3. The `modules/<name>.py` file is deleted
4. `main.py` is updated: inline nodes replace the `pipeline.submodel()` line

### 2.4 Renaming a Submodel

1. Right-click → **"Rename Submodel"**
2. The submodel file is renamed on disk
3. The import in `main.py` is updated

---

## 3. Code Representation

### 3.1 Main Pipeline File — `main.py`

```python
"""Pipeline: my_pipeline"""

import polars as pl
import haute

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.data_source(table="quotes.delta.policies", deploy_input=True, row_id_column="IDpol")
def policies() -> pl.LazyFrame:
    """data_source node"""
    from haute._databricks_io import read_cached_table
    return read_cached_table("quotes.delta.policies")


# Submodel: model_scoring
# Imports frequency_model, severity_model from modules/model_scoring.py
pipeline.submodel("modules/model_scoring.py")


@pipeline.transform
def calculate_premium(severity_model: pl.LazyFrame, frequency_model: pl.LazyFrame) -> pl.LazyFrame:
    """calculate_premium node"""
    df = (
        frequency_model
        .join(severity_model, on='IDpol', how='left')
        .with_columns(technical_price=pl.col('freq_preds') * pl.col('sev_preds'))
    )
    return df


@pipeline.output()
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output node"""
    return calculate_premium


# Wire nodes together
pipeline.connect("policies", "model_scoring")
pipeline.connect("model_scoring.frequency_model", "calculate_premium")
pipeline.connect("model_scoring.severity_model", "calculate_premium")
pipeline.connect("calculate_premium", "output")
```

### 3.2 Submodel File — `modules/model_scoring.py`

```python
"""Submodel: model_scoring"""

import polars as pl
import haute

submodel = haute.Submodel("model_scoring")


@submodel.external_file(path="models/freq.cbm", file_type="catboost", model_class="regressor")
def frequency_model(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Frequency model scoring"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("models/freq.cbm", "catboost", "regressor")
    features = policies.select(obj.feature_names_).collect().to_numpy()
    preds = obj.predict(features)
    df = policies.select('IDpol').with_columns(freq_preds=pl.Series(preds))
    return df


@submodel.external_file(path="models/sev.cbm", file_type="catboost", model_class="regressor")
def severity_model(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Severity model scoring"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("models/sev.cbm", "catboost", "regressor")
    features = policies.select(obj.feature_names_).collect().to_numpy()
    preds = obj.predict(features)
    df = policies.select('IDpol').with_columns(sev_preds=pl.Series(preds))
    return df
```

### 3.3 Design Decisions for Code Representation

| Decision | Choice | Rationale |
|---|---|---|
| **Submodel variable** | `submodel = haute.Submodel(name)` | Mirrors `pipeline = haute.Pipeline(name)` — consistent API |
| **Decorator** | `@submodel.<type>` | Mirrors `@pipeline.<type>` — same decorator API |
| **Import mechanism** | `pipeline.submodel("modules/path.py")` | Explicit path, no Python import magic, works with file watcher |
| **File location** | `modules/` directory | Convention, keeps root clean, file watcher already watches this |
| **Edge wiring** | Cross-boundary edges stay in `main.py` | The parent file owns all inter-submodel and submodel-to-parent wiring |
| **Submodel internal edges** | Written in the submodel file | Via `submodel.connect()` calls |
| **Inputs** | Inferred from function parameters | If `frequency_model(policies)` and `policies` is not defined in the submodel, it's an input |
| **Outputs** | Inferred from cross-boundary edges | If a parent node depends on `frequency_model`, it's an output |
| **Connect syntax** | `pipeline.connect("policies", "model_scoring")` for submodel-level wiring; or `pipeline.connect("policies", "frequency_model")` for transparent wiring | See §3.4 |

### 3.4 Edge Wiring Strategy: Transparent Flattening

Submodels are **transparent** for edge wiring — the parent pipeline can reference internal node names directly:

```python
# In main.py — edges reference internal nodes by name
pipeline.connect("policies", "frequency_model")
pipeline.connect("policies", "severity_model")
pipeline.connect("frequency_model", "calculate_premium")
pipeline.connect("severity_model", "calculate_premium")
```

This is the simplest approach: the parser flattens the submodel's nodes into the parent graph, and all edges use the same flat namespace. No dotted paths, no port mapping.

**Collision prevention**: `haute lint` will warn if two submodels define nodes with the same name. In future, we can add optional namespacing (`model_scoring.frequency_model`) but the flat approach works for v1.

---

## 4. Graph Model (React Flow JSON)

### 4.1 Collapsed View (Parent Graph)

When the graph is loaded, submodel child nodes are **not** in the parent graph. Instead, a single submodel node represents the group:

```json
{
  "nodes": [
    {
      "id": "policies",
      "type": "dataSource",
      "data": { "label": "policies", "nodeType": "dataSource", "config": { ... } }
    },
    {
      "id": "submodel__model_scoring",
      "type": "submodel",
      "data": {
        "label": "model_scoring",
        "nodeType": "submodel",
        "config": {
          "file": "modules/model_scoring.py",
          "childNodeIds": ["frequency_model", "severity_model"],
          "inputPorts": [
            { "name": "policies", "connectedFrom": "policies" }
          ],
          "outputPorts": [
            { "name": "frequency_model", "connectedTo": ["calculate_premium"] },
            { "name": "severity_model", "connectedTo": ["calculate_premium"] }
          ]
        }
      }
    },
    {
      "id": "calculate_premium",
      "type": "transform",
      "data": { ... }
    },
    {
      "id": "output",
      "type": "output",
      "data": { ... }
    }
  ],
  "edges": [
    { "id": "e_policies_submodel__model_scoring", "source": "policies", "target": "submodel__model_scoring", "sourceHandle": null, "targetHandle": "in__policies" },
    { "id": "e_submodel__model_scoring_calculate_premium_freq", "source": "submodel__model_scoring", "target": "calculate_premium", "sourceHandle": "out__frequency_model" },
    { "id": "e_submodel__model_scoring_calculate_premium_sev", "source": "submodel__model_scoring", "target": "calculate_premium", "sourceHandle": "out__severity_model" },
    { "id": "e_calculate_premium_output", "source": "calculate_premium", "target": "output" }
  ],
  "submodels": {
    "model_scoring": {
      "file": "modules/model_scoring.py",
      "childNodeIds": ["frequency_model", "severity_model"],
      "inputPorts": ["policies"],
      "outputPorts": ["frequency_model", "severity_model"]
    }
  }
}
```

### 4.2 Expanded View (Drill-Down)

When the user double-clicks the submodel, the frontend requests the submodel's internal graph:

```json
{
  "nodes": [
    { "id": "__port_in__policies", "type": "submodelPort", "data": { "portDirection": "input", "portName": "policies" } },
    { "id": "frequency_model", "type": "externalFile", "data": { ... } },
    { "id": "severity_model", "type": "externalFile", "data": { ... } },
    { "id": "__port_out__frequency_model", "type": "submodelPort", "data": { "portDirection": "output", "portName": "frequency_model" } },
    { "id": "__port_out__severity_model", "type": "submodelPort", "data": { "portDirection": "output", "portName": "severity_model" } }
  ],
  "edges": [
    { "source": "__port_in__policies", "target": "frequency_model" },
    { "source": "__port_in__policies", "target": "severity_model" },
    { "source": "frequency_model", "target": "__port_out__frequency_model" },
    { "source": "severity_model", "target": "__port_out__severity_model" }
  ]
}
```

### 4.3 Flat Graph (for Execution)

The executor, trace engine, and deploy pipeline receive a **flat graph** where submodels are dissolved:

```json
{
  "nodes": [
    { "id": "policies", ... },
    { "id": "frequency_model", ... },
    { "id": "severity_model", ... },
    { "id": "calculate_premium", ... },
    { "id": "output", ... }
  ],
  "edges": [
    { "source": "policies", "target": "frequency_model" },
    { "source": "policies", "target": "severity_model" },
    { "source": "frequency_model", "target": "calculate_premium" },
    { "source": "severity_model", "target": "calculate_premium" },
    { "source": "calculate_premium", "target": "output" }
  ]
}
```

This means `executor.py`, `trace.py`, and `deploy/` **never see submodel nodes** — they always receive the flat graph. The flattening happens in the API layer.

---

## 5. Backend Changes

### 5.1 `pipeline.py` — New `Submodel` Class

```python
class Submodel:
    """A reusable group of nodes, defined in a separate .py file."""

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self._nodes: list[Node] = []
        self._node_map: dict[str, Node] = {}
        self._edges: list[tuple[str, str]] = []

    def transform(self, fn=None):
        """Register a transform node. Same API pattern as Pipeline."""
        ...

    def data_source(self, **config):
        """Register a data source node."""
        ...

    def connect(self, source: str, target: str) -> Submodel:
        """Declare an edge within this submodel."""
        self._edges.append((source, target))
        return self
```

Add to `Pipeline`:
```python
class Pipeline:
    def submodel(self, file: str) -> None:
        """Import a submodel from a separate .py file."""
        self._submodel_files.append(file)
```

### 5.2 `parser.py` — Parse Submodel Imports

New functions:
- `_extract_submodel_imports(tree)` — find `pipeline.submodel("path")` calls
- `parse_submodel_file(filepath)` — like `parse_pipeline_file` but for `@submodel.<type>` decorators
- `_merge_submodel_nodes(parent_graph, submodel_graphs)` — flatten submodel nodes into parent

The parser needs two modes:
1. **Hierarchical parse** (for GUI): returns graph with submodel metadata, child nodes grouped
2. **Flat parse** (for execution): returns fully flattened graph

```python
def parse_pipeline_file(filepath, flatten=False):
    """Parse a pipeline file.

    Args:
        flatten: If True, dissolve submodels into flat graph (for executor/trace/deploy).
                 If False, keep submodel groupings (for GUI).
    """
```

Detailed changes to `parse_pipeline_source()`:
- After parsing `@pipeline.<type>` functions, scan for `pipeline.submodel(...)` calls
- For each submodel path, parse the submodel file
- In hierarchical mode: add a `submodel` node to the graph + store child graphs in `submodels` dict
- In flat mode: merge child nodes directly into the parent graph
- Detect which existing `pipeline.connect()` edges cross into/out of the submodel

Detection of `pipeline.submodel()` calls:
```python
def _extract_submodel_calls(tree: ast.Module) -> list[str]:
    """Find pipeline.submodel("path") calls and return the file paths."""
    paths = []
    for node in ast.iter_child_nodes(tree):
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr != "submodel":
            continue
        if call.args:
            val = _eval_ast_literal(call.args[0])
            if isinstance(val, str):
                paths.append(val)
    return paths
```

Submodel file parsing reuses the same `_is_pipeline_decorator` logic but matches `@submodel.<type>` instead of `@pipeline.<type>`:

```python
def _is_submodel_decorator(decorator: ast.expr) -> bool:
    """Check if a decorator is @submodel.<type> or @submodel.<type>(...)."""
    if isinstance(decorator, ast.Attribute):
        if isinstance(decorator.value, ast.Name):
            return decorator.value.id == "submodel"
    if isinstance(decorator, ast.Call):
        return _is_submodel_decorator(decorator.func)
    return False
```

### 5.3 `codegen.py` — Multi-File Code Generation

New function: `graph_to_code_multi(graph, ...)` that returns a dict of `{filepath: code}`:

```python
def graph_to_code_multi(
    graph: dict,
    pipeline_name: str = "main",
    description: str = "",
    preamble: str = "",
) -> dict[str, str]:
    """Generate code for a pipeline with submodels.

    Returns:
        Dict mapping relative file path → generated Python code.
        e.g. {"main.py": "...", "modules/model_scoring.py": "..."}
    """
```

Logic:
1. Separate nodes into groups: root-level nodes vs submodel children
2. For each submodel group:
   - Generate a `modules/<name>.py` file with `submodel = haute.Submodel(name)`
   - Include `@submodel.<type>` decorated functions
   - Include `submodel.connect()` calls for internal edges
3. For the root file:
   - Generate `pipeline.submodel("modules/<name>.py")` lines
   - Generate `@pipeline.<type>` decorated functions for root-level nodes
   - Generate `pipeline.connect()` calls for cross-boundary and root-level edges

### 5.4 `server.py` — New API Endpoints

```python
@app.post("/api/submodel/create")
async def create_submodel(body: CreateSubmodelRequest) -> CreateSubmodelResponse:
    """Group selected nodes into a submodel.

    1. Identify boundary edges (inputs/outputs)
    2. Generate submodel .py file
    3. Update main .py file
    4. Return updated parent graph with submodel node
    """

@app.get("/api/submodel/{name}")
async def get_submodel(name: str) -> dict:
    """Return the internal graph of a submodel (for drill-down view)."""

@app.post("/api/submodel/dissolve")
async def dissolve_submodel(body: DissolveSubmodelRequest) -> dict:
    """Ungroup a submodel back into the parent pipeline."""

@app.post("/api/pipeline/flatten")
async def flatten_pipeline() -> dict:
    """Return the fully flattened graph (submodels dissolved).
    Used by the frontend before calling execute/trace/preview.
    """
```

Modified endpoints:
- `save_pipeline` — calls `graph_to_code_multi()` for multi-file writes, marks all writes as self-write
- `run_pipeline` / `preview_node` / `trace_row` — flatten the graph before passing to executor
- `get_first_pipeline` / `get_pipeline` — returns hierarchical graph for the GUI

### 5.5 `server.py` — File Watcher Updates

The file watcher already watches `modules/` (line 663-665 in current server.py). When a submodel file changes:
1. Re-parse the full parent pipeline (which transitively parses the submodel)
2. Broadcast the updated hierarchical graph

The watcher needs one change: when a submodel file is modified, it should trigger a re-parse of the **parent pipeline file** (not just the changed file), because the GUI needs the full hierarchical graph.

### 5.6 `graph_utils.py` — Flatten Helper

New utility function:

```python
def flatten_graph(graph: dict) -> dict:
    """Dissolve all submodel nodes into a flat graph for execution.

    Replaces each submodel node with its child nodes and rewires
    the boundary edges to point to the actual internal nodes.
    """
```

The `graph_fingerprint` function should include submodel metadata so that changes inside a submodel invalidate the cache.

### 5.7 `schemas.py` — New Pydantic Models

```python
class CreateSubmodelRequest(BaseModel):
    name: str
    node_ids: list[str]  # IDs of nodes to group
    graph: Graph          # current graph state
    source_file: str = ""

class CreateSubmodelResponse(BaseModel):
    status: str = "ok"
    submodel_file: str     # e.g. "modules/model_scoring.py"
    parent_file: str       # e.g. "main.py"
    graph: dict            # updated parent graph with submodel node

class DissolveSubmodelRequest(BaseModel):
    submodel_name: str
    graph: Graph
    source_file: str = ""

class SubmodelPortInfo(BaseModel):
    name: str
    direction: str  # "input" | "output"
    connected_node: str
```

### 5.8 `discovery.py` — No Changes Needed

Submodel files are discovered transitively through the parser (via `pipeline.submodel()` calls), not independently. They don't contain `haute.Pipeline(...)` so `discover_pipelines()` correctly skips them.

### 5.9 `haute lint` — New Validation Rules

- Submodel files referenced in `pipeline.submodel()` must exist
- No node name collisions across submodels and parent
- Submodel files must contain `haute.Submodel(...)` (not `haute.Pipeline`)
- Internal submodel edges reference only nodes within that submodel
- Cross-boundary edges reference valid parent/child nodes

### 5.10 Deploy Pipeline — Minimal Changes

The deploy path calls `parse_pipeline_file()` then `prune_for_deploy()`. We just need the parser to return a flat graph for deploy:

```python
# deploy/_config.py — resolve_config()
full_graph = parse_pipeline_file(config.pipeline_file, flatten=True)
```

No other deploy changes needed — the flat graph is identical to what it would be without submodels.

---

## 6. Frontend Changes

### 6.1 New Components

#### `SubmodelNode.tsx` — Custom React Flow Node

```
┌────────────────────────────────┐
│ ○ input:policies    SUBMODEL   │
│                                │
│   📦 model_scoring             │
│   2 nodes                      │
│                                │
│       frequency_model ○        │
│       severity_model  ○        │
└────────────────────────────────┘
```

Features:
- Multiple named input handles (left side)
- Multiple named output handles (right side)
- Distinct visual style: dashed border, folder icon, different accent colour
- Node count badge
- Double-click handler → drill into submodel
- Supports trace highlighting (glows if any internal node is in trace path)

#### `SubmodelPortNode.tsx` — Port Nodes Inside Drill-Down

When viewing inside a submodel, input/output ports appear as special nodes:
- Input ports: left-aligned, represents data flowing in from parent
- Output ports: right-aligned, represents data flowing out to parent
- Non-editable, non-deletable
- Visual style: rounded pill shape, directional arrow

#### `BreadcrumbBar.tsx` — Navigation Breadcrumbs

```
my_pipeline  >  model_scoring
```

- Clickable segments to navigate back up
- Shows current navigation depth
- Escape key also navigates back

### 6.2 `App.tsx` — State Changes

New state:
```typescript
const [viewStack, setViewStack] = useState<ViewLevel[]>([
  { type: "pipeline", name: "main", file: "main.py" }
])
// viewStack[viewStack.length - 1] is the current view
// Pushing a submodel view = drilling in
// Popping = navigating back

interface ViewLevel {
  type: "pipeline" | "submodel"
  name: string
  file: string
  parentGraph?: { nodes: Node[]; edges: Edge[] }  // cached parent state
}
```

Modified behaviour:
- `onSelectionChange` → when a submodel node is selected, show submodel properties panel
- Double-click handler → drill into submodel (push to viewStack)
- Breadcrumb click → pop viewStack and restore parent graph
- `handleSave` → calls `graph_to_code_multi` via the save endpoint, handling multi-file writes
- `handleRun` / `fetchPreview` → flatten graph before sending to backend
- Context menu → add "Group as Submodel" when multiple nodes selected
- Context menu → add "Ungroup Submodel" when a submodel node is right-clicked

### 6.3 `NodePalette.tsx`

No changes needed in Phase 1. Submodels are created from existing nodes, not from the palette.

(Phase 2: could add "Import Submodel" item to palette for reusing existing submodels across pipelines.)

### 6.4 `NodePanel.tsx` — Submodel Properties

When a submodel node is selected, show:
- Submodel name (editable)
- Source file path
- Input ports list (inferred, read-only)
- Output ports list (inferred, read-only)
- Node count
- "Open" button (same as double-click)
- "Ungroup" button

### 6.5 `DataPreview.tsx` — No Changes

Preview works on the flat graph. When the user selects a submodel node in the collapsed view, preview shows the combined output of its output ports (or a summary). When inside the drill-down view, preview works normally on individual nodes.

### 6.6 `TracePanel.tsx` — Submodel Trace Steps

Trace works on the flat graph, so trace steps include internal submodel nodes. In the collapsed parent view, trace steps that belong to a submodel are **grouped under the submodel's step**:

```
policies → model_scoring [click to expand] → calculate_premium → output
```

Clicking the submodel trace step expands to show internal trace steps inline.

### 6.7 `utils/nodeTypes.ts` — Register Submodel Type

```typescript
export const nodeTypeIcons = {
  ...existing,
  submodel: Package,        // lucide Package icon
  submodelPort: ArrowRight, // lucide ArrowRight icon
}

export const nodeTypeColors = {
  ...existing,
  submodel: "#f97316",       // orange
  submodelPort: "#94a3b8",   // slate
}

export const nodeTypeLabels = {
  ...existing,
  submodel: "SUBMODEL",
  submodelPort: "PORT",
}
```

### 6.8 Graph Flattening (Frontend Helper)

The frontend needs a `flattenGraph()` utility that converts the hierarchical GUI graph to a flat graph before sending to execute/trace/preview endpoints:

```typescript
function flattenGraph(
  parentNodes: Node[],
  parentEdges: Edge[],
  submodelGraphs: Record<string, { nodes: Node[]; edges: Edge[] }>,
): { nodes: Node[]; edges: Edge[] } {
  // 1. Remove submodel nodes from parent
  // 2. Add child nodes from each submodel
  // 3. Rewire submodel boundary edges to internal nodes
  // 4. Add internal submodel edges
  // Return flat graph
}
```

Alternative: the flattening could happen server-side. The frontend sends the hierarchical graph, and the server flattens it before passing to executor/trace. **Server-side flattening is preferable** — it keeps the frontend simpler and ensures consistent behaviour.

---

## 7. Interaction With Existing Features

### 7.1 Tracing

| Aspect | Impact | Changes |
|---|---|---|
| **Trace engine** | None | Works on flat graph, unchanged |
| **Trace cache** | None | `graph_fingerprint` includes submodel nodes (they're in the flat graph) |
| **Trace panel (collapsed view)** | Medium | Group internal trace steps under submodel header |
| **Trace panel (drill-down view)** | None | Normal trace inside the submodel |
| **Graph highlighting** | Medium | In collapsed view, submodel node glows if any internal node is in trace path |

### 7.2 Lazy Execution / Preview

| Aspect | Impact | Changes |
|---|---|---|
| **Executor** | None | Works on flat graph, unchanged |
| **Preview cache** | None | Flat graph fingerprint handles this |
| **Preview (collapsed view)** | Low | Show output of the submodel's output nodes |
| **Preview (drill-down view)** | None | Normal preview on internal nodes |

### 7.3 Deploy

| Aspect | Impact | Changes |
|---|---|---|
| **Parser** | Low | `parse_pipeline_file(flatten=True)` for deploy path |
| **Pruner** | None | Operates on flat graph |
| **Scorer** | None | Operates on flat graph |
| **Bundler** | None | Artifacts collected from flat graph nodes |
| **Validators** | None | Operates on flat graph |

### 7.4 Code ↔ GUI Sync

| Aspect | Impact | Changes |
|---|---|---|
| **Code → GUI** | Medium | Parser must follow submodel imports, build hierarchical graph |
| **GUI → Code** | Medium | Codegen must produce multiple files |
| **File watcher** | Low | Already watches `modules/`; trigger parent re-parse on submodel change |
| **Self-write detection** | Low | Mark all multi-file writes with `_mark_self_write()` |

### 7.5 Copy/Paste/Undo

| Aspect | Impact | Changes |
|---|---|---|
| **Copy/Paste** | Low | A submodel node is copied as a single unit. Internal nodes are not directly copyable from the parent view |
| **Undo/Redo** | Medium | Submodel creation/dissolution must be undoable. This is a multi-file operation — undo must restore both `main.py` and `modules/<name>.py` |
| **Delete** | Low | Deleting a submodel node deletes the `.py` file and inlines nothing (nodes are lost). Warn the user first |

---

## 8. Edge Cases

### 8.1 Node Name Collisions

Two submodels might define nodes with the same name (e.g. both have a `transform` node). **Phase 1**: `haute lint` warns about collisions and blocks submodel creation if collisions exist. **Phase 2**: optional namespacing prefix (`model_scoring__transform`).

### 8.2 Circular Submodel References

`A.py` imports submodel `B.py` which imports submodel `A.py`. The parser must detect cycles and raise a clear error.

### 8.3 Empty Submodel

User groups zero or one node. Reject: submodels must contain ≥2 nodes.

### 8.4 Submodel With No External Connections

All nodes in the submodel are disconnected from the rest of the graph. Allow it (it's valid, just useless). Show a warning.

### 8.5 Nested Submodels

A submodel can contain a `submodel.submodel()` import for further nesting. **Phase 1**: disallow nesting — only one level deep. **Phase 2**: allow recursive nesting.

### 8.6 Editing Submodel File in IDE

User edits `modules/model_scoring.py` in VS Code. File watcher detects the change, re-parses the parent pipeline (which transitively re-parses the submodel), and broadcasts the updated hierarchical graph to the GUI. This already works because the file watcher watches `modules/`.

### 8.7 Partial Selection

User selects some nodes that form a valid subgroup but also some disconnected nodes. Allow it — the resulting submodel just has multiple disconnected branches internally.

### 8.8 Submodel Node With Mixed Edge Types

A node inside the submodel has both internal edges (to other submodel nodes) and external edges (to parent nodes). Both are handled correctly: internal edges go in the submodel file, external edges go in the parent file.

---

## 9. Implementation Phases

### Phase 1: Core Backend (Parser + Codegen + Submodel Class)

1. **`Submodel` class** in `pipeline.py` — mirror of `Pipeline` with `@submodel.<type>` decorators
2. **`__init__.py`** — export `Submodel` class
3. **Parser updates** — detect `pipeline.submodel()`, parse submodel files, merge nodes
4. **Parser `flatten` parameter** — flat mode for executor, hierarchical for GUI
5. **Codegen `graph_to_code_multi()`** — multi-file code generation
6. **`flatten_graph()` in `graph_utils.py`** — dissolve submodel nodes to flat graph
7. **Tests** — parser round-trip tests, codegen multi-file tests

### Phase 2: Server API Layer

8. **New schemas** in `schemas.py` — request/response models
9. **`POST /api/submodel/create`** — create submodel from selected nodes
10. **`GET /api/submodel/{name}`** — return submodel internal graph
11. **`POST /api/submodel/dissolve`** — ungroup submodel
12. **Modify `save_pipeline`** — multi-file write support
13. **Modify `run_pipeline` / `preview_node` / `trace_row`** — flatten before execution
14. **File watcher** — trigger parent re-parse on submodel file change

### Phase 3: Frontend — SubmodelNode + Drill-Down

15. **`SubmodelNode.tsx`** — new React Flow node type with multi-handle support
16. **`SubmodelPortNode.tsx`** — port nodes for drill-down view
17. **`BreadcrumbBar.tsx`** — navigation breadcrumbs
18. **Register in `nodeTypes.ts`** — icon, colour, label for submodel type
19. **Register in `App.tsx`** — add to `nodeTypes` map

### Phase 4: Frontend — Creation + Dissolution UX

20. **Context menu** — "Group as Submodel" option for multi-select
21. **Context menu** — "Ungroup Submodel" for submodel nodes
22. **Submodel creation dialog** — name input, validation
23. **App.tsx state** — `viewStack` for drill-down navigation
24. **Double-click handler** — drill into submodel
25. **Escape handler** — navigate back

### Phase 5: Frontend — Integration

26. **NodePanel.tsx** — submodel properties section
27. **TracePanel.tsx** — grouped trace steps for submodel nodes
28. **Graph flattening** — server-side flattening before executor calls
29. **Preview** — submodel node preview (show output ports' data)
30. **Undo/redo** — multi-file operations

### Phase 6: Polish + Validation

31. **`haute lint`** — submodel-specific validation rules
32. **Node name collision detection**
33. **Circular reference detection**
34. **Edge case handling** (empty submodel, nested submodel block, etc.)
35. **E2E tests** — full round-trip: create submodel in GUI → check files on disk → edit file in IDE → verify GUI updates

---

## 10. File Inventory — What Changes

| File | Type of Change | Scope |
|---|---|---|
| `src/haute/pipeline.py` | New `Submodel` class + `Pipeline.submodel()` | Medium |
| `src/haute/__init__.py` | Export `Submodel` | Tiny |
| `src/haute/parser.py` | Submodel detection, recursive parsing, flatten param | Large |
| `src/haute/codegen.py` | `graph_to_code_multi()`, submodel file generation | Large |
| `src/haute/graph_utils.py` | `flatten_graph()` helper | Medium |
| `src/haute/schemas.py` | New request/response models | Small |
| `src/haute/server.py` | New endpoints, modified save/run/preview/trace | Medium |
| `src/haute/executor.py` | **No changes** (works on flat graph) | None |
| `src/haute/trace.py` | **No changes** (works on flat graph) | None |
| `src/haute/deploy/_config.py` | Pass `flatten=True` to parser | 1 line |
| `src/haute/deploy/_scorer.py` | **No changes** | None |
| `src/haute/cli.py` | `haute lint` submodel rules | Small |
| `frontend/src/nodes/SubmodelNode.tsx` | **New file** | Medium |
| `frontend/src/nodes/SubmodelPortNode.tsx` | **New file** | Small |
| `frontend/src/components/BreadcrumbBar.tsx` | **New file** | Small |
| `frontend/src/App.tsx` | viewStack, drill-down, context menu, flatten | Large |
| `frontend/src/panels/NodePanel.tsx` | Submodel properties section | Medium |
| `frontend/src/panels/TracePanel.tsx` | Grouped trace steps | Medium |
| `frontend/src/utils/nodeTypes.ts` | Add submodel + submodelPort | Small |
| `tests/test_parser.py` | Submodel parse tests | Medium |
| `tests/test_codegen.py` | Multi-file codegen tests | Medium |
| `tests/test_submodel.py` | **New file** — E2E submodel tests | Large |

---

## 11. Resolved Questions

1. **Reusability across pipelines?** No — a submodel belongs to one pipeline. Keep it simple.
2. **Submodel nodes in NodePalette?** No — submodels are created from existing nodes only.
3. **Keyboard shortcut for grouping?** `Ctrl+G` — no conflict with existing shortcuts (checked: Ctrl+S, Ctrl+Z, Ctrl+Y, Ctrl+C, Ctrl+V, Ctrl+A, Ctrl+1 are taken).
4. **Maximum nesting depth?** 1 level only — no nested submodels.
5. **Submodel preview in collapsed view?** Option B — show a summary table combining output port data.
