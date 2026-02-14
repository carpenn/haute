# Alteryx-Style Feature Plan for Runway

## What Alteryx Gets Right (and how we adapt it)

### 1. Instant Data Preview

**Alteryx**: Click any node → see the output DataFrame in a table below the canvas. Every node caches its result so you can browse the data at each step. This is the #1 feature that makes Alteryx sticky.

**Runway adaptation**:
- Bottom panel shows a paginated data table when you click a node
- Backend runs the pipeline up to the selected node and returns the preview
- `POST /api/pipeline/preview` — accepts the graph + target node ID, executes up to that node, returns first N rows
- Cache results per node so re-clicking is instant (invalidate when upstream changes)
- Show row count, column count, dtypes in a summary bar above the table

### 2. Left-Side Tool Palette (Categorized)

**Alteryx**: Tools grouped into categories — Input/Output, Preparation, Join, Parse, Transform, Predictive, etc. Each category has a distinct color and icon.

**Runway adaptation**:
- **Data** — Data Source (flat file, Databricks table, API)
- **Transform** — Filter, Select, Rename, With Columns, Group By, Sort
- **Join** — Join, Union/Concat, Anti-Join
- **Model** — Score Model (GLM, ML), Rating Table Lookup
- **Output** — Write file, Push to Databricks, Export

For now, keep it simple with the 5 types we have. Expand into sub-types later. Each category gets a color (already done) and an icon.

### 3. Configuration Panel (Right Side)

**Alteryx**: Click a node → right panel shows configuration fields specific to that tool type. Drop-downs, file pickers, expression editors, column selectors.

**Runway adaptation** (partially built):
- DataSource: source type toggle, file browser, schema preview ✅
- Transform: Polars code editor ✅
- **Add**: Column picker (multi-select from upstream schema) for Select/Filter nodes
- **Add**: Expression builder with autocomplete for column names
- **Add**: Join config — pick left/right keys from dropdowns

### 4. Run → See Results Immediately

**Alteryx**: Hit the play button → entire pipeline executes → every node shows a green check or red X → click any node to see its output.

**Runway adaptation**:
- "Run" button in the header (alongside Save)
- `POST /api/pipeline/run` — executes the full pipeline, returns status per node
- Each node on the canvas shows a status indicator: ⏳ running, ✅ done, ❌ error
- Click a node after run → bottom panel shows its cached output
- Errors show inline on the node with the traceback in the config panel

### 5. Connections = Data Flow

**Alteryx**: Wires between tools define data flow. Output anchor → input anchor. Some tools have multiple input anchors (Join has Left + Right).

**Runway adaptation** (built):
- Edges define data flow ✅
- `pipeline.connect("source", "target")` ✅
- Fan-out: one output feeds multiple downstream ✅
- Fan-in: multiple inputs auto-concat ✅
- **Future**: Join node with explicit Left/Right input handles

### 6. Undo/Redo

**Alteryx**: Full undo/redo stack for canvas operations.

**Runway adaptation**:
- React Flow supports this natively — wire up Ctrl+Z / Ctrl+Y
- Store a history stack of (nodes, edges) snapshots
- Undo button in the toolbar

### 7. Annotations on Canvas

**Alteryx**: You can add text boxes and comments directly on the canvas. Each tool also shows a small annotation below it.

**Runway adaptation**:
- Show the node name + description below each node on the canvas
- Add a "Comment" node type that's just a text box (non-executable)
- Tool tips on hover showing the node's config summary

---

## What Alteryx Gets Wrong (and how we fix it)

### 1. Proprietary, Expensive, Windows-Only

**Problem**: Alteryx costs $5,000+/year per seat. Windows desktop app. No version control, no CI/CD, no collaboration.

**Runway fix**:
- Open-source, free, runs anywhere Python runs
- Code is the source of truth → works with Git
- GUI is a web app → works on any OS
- Pipeline `.py` files are reviewable, diffable, testable

### 2. No Code Behind the GUI

**Problem**: Alteryx workflows are opaque XML blobs. You can't read them, diff them, or run them without the GUI. If Alteryx changes its format, your workflows are trapped.

**Runway fix**:
- Every pipeline is a valid `.py` file that runs standalone: `python my_pipeline.py`
- GUI edits save to readable Python code + JSON layout sidecar
- You can write pipelines in code and see them in the GUI, or build in the GUI and get code out
- No vendor lock-in — it's just Polars + Python

### 3. Terrible Performance at Scale

**Problem**: Alteryx uses its own data engine which chokes on large datasets (10M+ rows). Memory management is poor. No distributed computing.

**Runway fix**:
- Built on Polars — lazy evaluation, streaming, multi-threaded, written in Rust
- Same pipeline works on 1 row (live scoring) or 100M rows (batch)
- Can push heavy work to Databricks/Spark when needed
- Native Parquet support — no CSV bottlenecks

### 4. Expression Language is Non-Standard

**Problem**: Alteryx has its own formula language that nobody knows outside of Alteryx. It's not SQL, not Python, not R. Learning it is wasted effort.

**Runway fix**:
- You write real Polars/Python — transferable skills
- `pl.col("age") > 30` instead of `[Age] > 30`
- Full power of the Python ecosystem available in every node
- IDE autocomplete, linting, debugging all work

### 5. No Version Control or Collaboration

**Problem**: Alteryx workflows are binary-ish files. Git diffs are useless. Two people can't work on the same workflow. No branching/merging.

**Runway fix**:
- Pipeline is a `.py` file → Git-native
- PR reviews show exactly what changed in the pipeline logic
- Multiple people can work on different nodes/pipelines
- CI/CD: `runw run` in your pipeline to test on every commit

### 6. No Testing Framework

**Problem**: No way to unit test individual Alteryx tools. No assertions. No regression testing. You just eyeball the output.

**Runway fix**:
- Each node is a Python function → testable with pytest
- `pipeline.score(test_df)` runs the pipeline on test data
- Add assertions: row counts, column presence, value ranges
- Future: built-in `runw test` command with data quality checks

### 7. Slow Iteration Loop

**Problem**: Alteryx requires running the entire workflow to see results. No partial execution. No hot-reload.

**Runway fix**:
- Preview runs pipeline up to the selected node only (not the full DAG)
- File watcher + WebSocket for live sync between IDE and GUI
- Hot module reload in dev mode (already working via Vite)

### 8. No Live Scoring / API Deployment

**Problem**: Alteryx workflows can't be deployed as APIs. You have to buy Alteryx Server ($$$) for scheduling.

**Runway fix**:
- `pipeline.score(df)` works on any DataFrame — embed in a FastAPI endpoint
- Same pipeline code for batch and real-time
- Deploy as a standard Python web service
- Future: `runw deploy` to push to Databricks serving endpoint

---

## Implementation Priority

### Phase 1 — Core Loop (current)
- [x] Node palette with drag-and-drop
- [x] Canvas with connections
- [x] Config panel (DataSource file picker, Transform code editor)
- [x] Save → generates .py + .json
- [x] Edge-aware execution with `pipeline.connect()`
- [ ] **Bottom data preview panel** ← next

### Phase 2 — Run & Preview
- [ ] Run button → execute full pipeline
- [ ] Per-node status indicators (running/done/error)
- [ ] Click node → see its output in bottom panel
- [ ] Preview endpoint: run up to selected node

### Phase 3 — Richer Node Types
- [ ] Column picker (multi-select from upstream schema)
- [ ] Join node with Left/Right input handles
- [ ] Group By node with aggregation config
- [ ] Filter node with expression builder
- [ ] Output node with write-to-file/Databricks options

### Phase 4 — Developer Experience
- [ ] Undo/redo
- [ ] Keyboard shortcuts (delete node, duplicate, select all)
- [ ] Canvas annotations / comments
- [ ] Mini-map improvements
- [ ] Auto-layout (dagre algorithm)

### Phase 5 — Production Features
- [ ] `runw test` — run pipeline with assertions
- [ ] `runw deploy` — push to Databricks serving
- [ ] File watcher + WebSocket for IDE↔GUI live sync
- [ ] Pipeline versioning / history
