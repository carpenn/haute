# Runway — Architecture & Plan

**Open-source pricing engine for insurance teams on Databricks**

---

## 1. Vision

Runway is an open-source Python library that gives insurance pricing teams a **code-first, GUI-friendly** way to build, test, and deploy pricing pipelines. It bridges the gap between:

- **Pricing analysts** who are comfortable with visual tools (like WTW Radar)
- **Engineering best practices** that come from working in code: version control, CI/CD, unit tests, linting, code review

The core principle: **Python code is the source of truth**. The GUI is a live, editable view of that code. Edit either one — the other stays in sync.

Runway leans heavily into the **Databricks/MLflow ecosystem** rather than reinventing model training, registry, or serving.

---

## 2. What Runway Is (and Isn't)

### Runway IS:
- A Python DSL for defining pricing pipelines as code
- A browser-based React Flow UI for visualising and editing those pipelines
- A thin orchestration layer over MLflow (experiment tracking, model registry) and Databricks (model serving, data)
- A CLI that scaffolds projects with CI/CD, linting, tests, and deployment config out of the box
- An opinionated framework that makes it hard to do the wrong thing

### Runway IS NOT:
- A model training framework (use MLflow, scikit-learn, XGBoost, LightGBM, etc.)
- A replacement for Databricks (it's a client, not a platform)
- A proprietary black box — everything is `.py` files on disk

---

## 3. Core Concepts

### 3.1 Pipeline

A **Pipeline** is a directed acyclic graph (DAG) of **Nodes**. It represents the full journey from raw data to a deployable price.

```python
from runw import Pipeline, DataSource, Transform, Model, RatingStep, Output

pipeline = Pipeline(name="motor_pricing_v2")
```

### 3.2 Nodes

Nodes are the building blocks. Each node is a Python class with defined inputs, outputs, and logic.

| Node Type | Purpose | Example |
|---|---|---|
| **DataSource** | Connect to data (local CSV/Parquet, Databricks table, SQL) | `DataSource("policies", table="catalog.schema.policies")` |
| **Transform** | Data processing / feature engineering | `Transform("vehicle_age", fn=lambda df: df.with_columns(...))` |
| **ModelScore** | Score records using an MLflow registered model | `ModelScore("frequency_glm", model_uri="models:/freq_glm/Production")` |
| **RatingStep** | Individual rating operation (lookup, factor, cap/floor, load/discount) | `RatingStep("area_factor", lookup="area_table", key="postcode")` |
| **Blender** | Combine/weight multiple model outputs | `Blender("blended_frequency", weights={...})` |
| **Output** | Final output / price assembly | `Output("technical_price", formula="freq * sev * expense_load")` |

### 3.3 Shared Components

**Transforms** and **RatingSteps** can be defined once and reused across multiple pipelines. This maps to the user's requirement for shareable data processing.

```python
# shared/transforms.py
from runw import Transform

clean_vehicle = Transform(
    "clean_vehicle",
    fn=vehicle_cleaning_function,
    description="Standardise vehicle make/model codes"
)

# pipelines/motor.py
from shared.transforms import clean_vehicle

pipeline = Pipeline("motor")
pipeline.add(data_source >> clean_vehicle >> model_score >> output)
```

### 3.4 Bidirectional Code ↔ GUI

```
┌──────────────────┐         ┌──────────────────┐
│   .py files      │ ──parse──▶  React Flow      │
│   (source of     │         │  graph UI         │
│    truth)        │ ◀──gen───  (editable)       │
└──────────────────┘         └──────────────────┘
```

**Code → GUI:** Runway parses pipeline `.py` files (via AST inspection or a lightweight registry) and renders them as a React Flow graph.

**GUI → Code:** When a user adds/edits/connects nodes in the GUI, Runway writes valid Python code back to the `.py` files on disk. The generated code is clean, idiomatic, and diffable in git.

### 3.5 Project Structure (what `runw init` creates)

```
my-pricing-project/
├── pipelines/              # Pipeline definitions (.py)
│   └── motor.py
├── shared/                 # Shared transforms, rating tables, etc.
│   ├── transforms.py
│   └── tables/
│       └── area_factors.csv
├── tests/                  # pytest tests (auto-generated stubs)
│   └── test_motor.py
├── deployment/             # Deployment configs
│   └── databricks.yaml
├── .github/
│   └── workflows/
│       └── ci.yml          # Pre-configured GitHub Actions
├── pyproject.toml          # Project config (runw as dependency)
├── .pre-commit-config.yaml # ruff, mypy, runw lint
├── .python-version         # 3.13
├── uv.lock
└── README.md
```

---

## 4. Technical Architecture

### 4.1 Distribution

Single Python wheel with bundled React static assets (same model as atelier).

```
pip install runw
# or
uv add runw
```

### 4.2 System Architecture

```
┌──────────────────────────────────────────┐
│  Browser (React + TypeScript + React Flow)│
│  ┌───────────┬───────────┬──────────────┐│
│  │ Pipeline  │ Node      │ Properties   ││
│  │ Graph     │ Palette   │ Panel        ││
│  │ (React    │           │              ││
│  │  Flow)    │           │              ││
│  └───────────┴───────────┴──────────────┘│
│          REST + WebSocket                 │
└──────────────┬───────────────────────────┘
               │
┌──────────────┴───────────────────────────┐
│  Python Backend (FastAPI + uvicorn)       │
│  ┌─────────────────────────────────────┐ │
│  │  Pipeline Engine                    │ │
│  │  - Parse .py files → graph model    │ │
│  │  - Generate .py files ← graph edits │ │
│  │  - Execute pipelines locally        │ │
│  │  - Validate pipelines               │ │
│  └─────────────────────────────────────┘ │
│  ┌─────────────────────────────────────┐ │
│  │  MLflow / Databricks Client         │ │
│  │  - Model registry queries           │ │
│  │  - Model serving deployment         │ │
│  │  - Batch scoring (local or remote)  │ │
│  │  - Experiment tracking              │ │
│  └─────────────────────────────────────┘ │
│  ┌─────────────────────────────────────┐ │
│  │  File Watcher                       │ │
│  │  - Watch .py files for changes      │ │
│  │  - Push updates to UI via WebSocket │ │
│  └─────────────────────────────────────┘ │
│  Storage: local filesystem + SQLite      │
└──────────────────────────────────────────┘
               │
┌──────────────┴───────────────────────────┐
│  Databricks / MLflow (remote)            │
│  - Unity Catalog (data)                  │
│  - MLflow Model Registry                 │
│  - Model Serving endpoints               │
│  - Spark (batch scoring)                 │
└──────────────────────────────────────────┘
```

### 4.3 Backend Stack

| Concern | Library | Notes |
|---|---|---|
| **Language** | Python >= 3.11 | |
| **Package manager** | uv | |
| **Build system** | Hatchling | |
| **Web framework** | FastAPI | |
| **ASGI server** | uvicorn | |
| **WebSockets** | websockets | File watcher → UI sync |
| **Validation** | Pydantic v2 | |
| **DataFrames** | Polars | Fast, no pandas dependency |
| **CLI** | Click | `runw init`, `runw serve`, `runw deploy`, `runw lint` |
| **MLflow client** | mlflow | Model registry, tracking, serving |
| **Databricks client** | databricks-sdk | Unity Catalog, Model Serving, jobs |
| **AST / code gen** | libcst | Concrete syntax tree — parse & modify Python preserving formatting |
| **File watching** | watchfiles | Detect .py changes, push to UI |

### 4.4 Frontend Stack

| Concern | Library | Notes |
|---|---|---|
| **Language** | TypeScript ~5.9 | |
| **UI framework** | React 19 | |
| **Graph/flow editor** | React Flow | Core visual pipeline editor |
| **Routing** | react-router-dom | |
| **Bundler** | Vite | |
| **Styling** | Tailwind CSS v4 | |
| **Icons** | lucide-react | |
| **Components** | shadcn/ui | Pre-built accessible components |
| **State management** | Zustand | Lightweight, works well with React Flow |

### 4.5 Key Technical Decisions

- **libcst over AST**: We use [libcst](https://github.com/Instagram/LibCST) (concrete syntax tree) rather than Python's `ast` module because libcst preserves comments, formatting, and whitespace — critical for round-tripping code without mangling it.
- **Polars over pandas**: Faster, more memory efficient, better API.
- **Zustand over Redux**: Simpler state management, pairs well with React Flow's internal state.
- **File watcher for sync**: When a user edits `.py` files in their IDE, the file watcher detects changes and pushes updated graph state to the React Flow UI via WebSocket. This enables true bidirectional editing.

---

## 5. Scoring & Deployment

### 5.0 Core Mental Model

A pipeline is a small data transformation: **input → Polars DataFrame → pipeline nodes → output DataFrame**.

The same pipeline code runs in both modes — only the input differs:

| Mode | Input | DataFrame shape | Use case |
|---|---|---|---|
| **Live (API)** | Single JSON/XML request | 1 row | Real-time quoting via Databricks Model Serving |
| **Batch (offline)** | Polars DataFrame / Parquet / Databricks table | N rows | Batch scoring, optimisation, back-testing |

In live mode, the incoming request is parsed into a 1-row Polars DataFrame, passed through the pipeline, and the result is returned as JSON. In batch mode, the same pipeline processes an N-row DataFrame.

### 5.1 Local Scoring

```python
import runw
import polars as pl

pipeline = runw.Pipeline.load("pipelines/motor.py")

# Batch — N rows
quotes = pl.read_parquet("data/quotes_2025.parquet")
results = pipeline.score(quotes)  # Polars DataFrame in, Polars DataFrame out

# Single quote — same code, 1 row
single = pl.DataFrame({"vehicle_age": [3], "postcode": ["SW1A"], "driver_age": [35]})
price = pipeline.score(single)
```

### 5.2 Live Scoring (API)

When deployed to Databricks Model Serving, the pipeline receives a JSON request, converts it to a 1-row Polars DataFrame internally, runs the pipeline, and returns JSON:

```
POST /serving-endpoints/motor-pricing/invocations
{"vehicle_age": 3, "postcode": "SW1A", "driver_age": 35}
→ {"technical_price": 412.50}
```

### 5.3 Real-time API (via Databricks Model Serving)

```bash
runw deploy motor_pricing_v2 --target databricks --endpoint motor-pricing
```

This:
1. Packages the pipeline as an MLflow pyfunc model
2. Registers it in MLflow Model Registry
3. Creates/updates a Databricks Model Serving endpoint
4. Returns the endpoint URL

The pricing pipeline itself becomes the MLflow model — it wraps model lookups, rating steps, and business logic into a single deployable unit.

### 5.4 Deployment Configuration

```yaml
# deployment/databricks.yaml
endpoint:
  name: motor-pricing-v2
  compute:
    size: Small
    scale_to_zero: true
  environment:
    mlflow_model: pipelines/motor.py
    model_version: Production  # or a specific version
  
databricks:
  host: ${DATABRICKS_HOST}
  token: ${DATABRICKS_TOKEN}     # or use OAuth / service principal
  catalog: pricing
  schema: models
```

---

## 6. Engineering Practices (Baked In)

### 6.1 What `runw init` gives you for free

| Practice | Implementation |
|---|---|
| **Version control** | Git-native — everything is `.py` files, diffable and reviewable |
| **CI/CD** | Pre-configured GitHub Actions: lint → test → deploy on merge to main |
| **Linting** | `ruff` for Python, `runw lint` for pipeline-specific validation |
| **Type checking** | `mypy` (strict mode) |
| **Testing** | `pytest` stubs auto-generated for each pipeline; `runw test` runs them |
| **Pre-commit hooks** | ruff, mypy, runw lint — runs on every commit |
| **Dependency management** | `uv` with lockfile |

### 6.2 `runw lint` — Pipeline-specific checks

- All nodes have unique names
- No disconnected nodes in the pipeline graph
- All referenced MLflow models exist in the registry
- All referenced data sources are accessible
- Rating table files exist and have expected columns
- No circular dependencies

### 6.3 `runw test` — Auto-generated tests

```python
# tests/test_motor.py (auto-generated, user can extend)
from runw.testing import PipelineTestCase

class TestMotorPipeline(PipelineTestCase):
    pipeline = "pipelines/motor.py"
    
    def test_pipeline_loads(self):
        """Pipeline parses and all nodes resolve."""
        self.assert_pipeline_valid()
    
    def test_score_sample(self):
        """Score a sample record and check output shape."""
        result = self.score(self.sample_record())
        self.assert_has_output("technical_price", result)
```

---

## 7. CLI Commands

| Command | Description |
|---|---|
| `runw init [name]` | Scaffold a new pricing project |
| `runw serve` | Start the browser UI (FastAPI + React Flow) |
| `runw score <pipeline> <data>` | Score data locally |
| `runw deploy <pipeline>` | Deploy to Databricks Model Serving |
| `runw lint` | Validate pipelines |
| `runw test` | Run pipeline tests |
| `runw status` | Show deployed endpoints and their status |

---

## 8. Design Decisions (Finalised)

### 8.1 Code ↔ GUI granularity → Hybrid
Coarse pipeline stages by default, expandable to fine-grained operations. Users see a clean high-level graph and can drill into any node to see/edit individual operations.

### 8.2 Code generation strategy → Decorators + Declarative
Each decorated block of code (`@runw.node`) corresponds to a node in the GUI. Code is organised into sections using decorators. Simple nodes can use a declarative API, complex nodes use decorated Python functions. Low floor, high ceiling.

```python
import runw

@runw.node
def clean_vehicle(df: pl.DataFrame) -> pl.DataFrame:
    """Standardise vehicle make/model codes."""
    return df.with_columns(...)

@runw.node
def score_frequency(df: pl.DataFrame) -> pl.DataFrame:
    """Score using the frequency GLM from MLflow."""
    model = runw.mlflow_model("models:/freq_glm/Production")
    return df.with_columns(pred_freq=model.predict(df))
```

### 8.3 Rating tables → All in Databricks
All rating tables (even small ones) live in Databricks Unity Catalog. Referenced by `catalog.schema.table` URI. Viewable/editable in the GUI. This keeps a single source of truth and avoids CSV sprawl in git.

### 8.4 Shared preprocessing logic
Preprocessing transforms (e.g., categorical grouping) are defined once in `shared/` and reused across both modelling pipelines (for training) and rating pipelines (for deployment). The rating pipeline is the deployable unit; modelling pipelines are a separate workflow that produces MLflow-registered models.

### 8.5 PyPI package name → `runw`
Short, available, memorable. CLI command: `runw`. Import: `import runw`.

---

## 9. Killer Features — What Gets Attention

The features below are ordered by impact. Features 1–2 are what get **attention** (demos, HN, LinkedIn). Features 3–4 are what get **adoption** (teams choosing runw over alternatives). Features 5–6 are what make engineering teams **insist** on using it.

### 9.1 Live Code ↔ GUI Sync (the headline feature)

Edit a `.py` file in VS Code → the React Flow graph updates in real-time. Edit the graph → the `.py` file updates on disk. No other tool does this well. This is the feature that sells itself in a 30-second demo.

Implementation: libcst for round-trip-safe Python parsing. File watcher (watchfiles) detects `.py` changes, pushes updated graph state to the React Flow UI via WebSocket. GUI edits generate clean, idiomatic, diffable Python code back to disk.

The hard part isn't parsing — it's **conflict resolution** when both sides edit simultaneously. Treat the `.py` file as the single source of truth; GUI edits write to disk immediately, and the file watcher debounces to avoid loops.

### 9.2 What-If Sensitivity Mode (the "wow" demo feature)

Pin a single input row and show sliders for each input variable. Drag "driver age" from 25 to 45 and watch the price update **live** through every node in the graph. Each node shows its intermediate output updating in real-time.

This is what pricing analysts do all day in spreadsheets. Making it visual and instant in the pipeline graph is a **demo moment** — the kind of thing people share.

Implementation: Since the pipeline uses Polars lazy evaluation, scoring a single row is near-instant. The frontend sends a modified 1-row DataFrame on each slider change, the backend runs the pipeline and returns per-node results. Debounce slider input to ~100ms.

### 9.3 Execution Trace / Data Lineage (the regulatory feature)

Click any cell in the output preview table → highlight the path through every node that produced it, showing intermediate values at each step.

Example: click a price of £412.50 and see:
```
base rate £300 → area factor ×1.2 → NCD ×0.85 → frequency load ×1.35 → £412.50
```
traced visually through the graph with each node lit up.

This is **regulatory gold** for insurance (Solvency II, IFRS 17 require explainability). No open-source pricing tool does this. Achievable because we already have per-node lazy execution and preview results.

#### Implementation status

**Phase A (done)** — Foundation in `src/runw/trace.py`:
- `execute_trace()` runs the pipeline on a single row and captures per-node input/output snapshots
- `SchemaDiff` classifies columns at each node as `added`, `removed`, `modified`, or `passed_through`
- `TraceStep` / `TraceResult` dataclasses carry the full trace payload
- Column filtering: pass a `column` name to prune the trace to only nodes that touch it
- `POST /api/pipeline/trace` endpoint wired up in `server.py`
- Reuses the existing `executor._build_node_fn` infrastructure — no duplication

**TODO — future phases** (see `docs/EXECUTION_TRACE_DESIGN.md` for full design):
- [ ] Row-identity tracking (`__trace_row_id`) for filters, joins, sorts
- [ ] `JoinInfo` / `AggregationInfo` for cardinality-changing nodes
- [ ] Column provenance via Polars expression plan inspection
- [ ] Human-readable expression generation ("base × area × ncd = £412")
- [ ] Compare-trace: two rows side-by-side with per-node diff
- [ ] Frontend trace panel (highlight path on graph, value badges on nodes)
- [ ] `runw trace export` CLI for regulatory PDF/HTML reports
- [ ] Trace caching (LRU for recent row traces, cached schema diffs)

### 9.4 One-Command Deploy to Databricks (the adoption feature)

```bash
runw deploy motor --endpoint motor-pricing
```

Three commands from init to live API:
```bash
runw init motor
# ... edit pipeline ...
runw deploy motor --endpoint motor-pricing
```

This packages the pipeline as an MLflow pyfunc model, registers it in MLflow Model Registry, creates/updates a Databricks Model Serving endpoint, and returns the URL. Pricing teams spend **weeks** on deployment plumbing — this makes it a one-liner.

### 9.5 Pipeline Visual Diff (the engineering team feature)

```bash
runw diff HEAD~1
```

Renders a side-by-side graph diff: green nodes = added, red = removed, amber = changed (with inline code diff on hover). Turns every PR review into a visual experience.

Git diffs of pipeline `.py` files are hard to review; a graph diff is immediately legible. This is genuinely novel — no pipeline tool does this.

### 9.6 Natural Language → Polars Code (the adoption accelerator)

Add a "describe what you want" input to the Polars node. Type:
> "join on postcode and calculate average claim cost by area"

→ generates the Polars code automatically.

This massively lowers the barrier for pricing analysts who know SQL/Excel but can't write Polars. Implementation: call an LLM API (OpenAI/Anthropic) with a well-crafted prompt that includes the input schema (already available from lazy scan) and available column names. No need to build an LLM — just a smart prompt.

### 9.7 Rating Table Hot-Reload with Impact Preview

Edit a rating factor table in Databricks → instantly see how it changes output prices across the preview dataset. Show a histogram: "this change increases 12% of quotes by >5%".

This is the #1 workflow in pricing. Making it instant and visual (instead of re-running a notebook) is a genuine competitive advantage over legacy tools.

### 9.8 Schema Validation Between Nodes

Automatically check that the output columns of node A match the expected input of node B. Show a **red edge** if there's a mismatch. Catch "column not found" errors before execution, not during.

Low-hanging fruit with high impact — the schema is already available from Polars lazy scans. This is the kind of polish that makes the tool feel professional.

---

## 10. Phased Roadmap

### Phase 1 — Hello World UI ✅
- [x] Scaffold project (pyproject.toml, frontend/, src/runw/)
- [x] FastAPI backend with pipeline API endpoints
- [x] React Flow frontend rendering a pipeline graph
- [x] `runw serve` CLI command opens browser
- [x] Node palette, drag-and-drop, context menu, keyboard shortcuts
- [x] Data preview panel with resizable split
- [x] Polars lazy execution with configurable row limit
- [x] Dark theme, polished UI
- [ ] Static asset bundling (frontend builds into Python wheel)

### Phase 2 — Live Code ↔ GUI Sync
- [ ] Parse decorated `.py` files → React Flow graph (libcst)
- [ ] GUI edits write back to `.py` files (clean, diffable code)
- [ ] File watcher pushes changes to UI via WebSocket
- [ ] Conflict resolution (file = source of truth, debounced sync)
- [ ] Schema validation between connected nodes (red edge on mismatch)

### Phase 3 — Deploy & Score
- [ ] Package a pipeline as an MLflow pyfunc model
- [ ] `runw deploy` registers model and deploys to Databricks Model Serving
- [ ] Local scoring engine (`pipeline.score(df)` for dev/testing)
- [ ] DataSource node with Databricks Unity Catalog support
- [ ] Rating table viewer (reads from Databricks)

### Phase 4 — Killer Demo Features
- [ ] What-if sensitivity mode (slider-driven single-row scoring)
- [x] Execution trace / data lineage — Phase A: single-row trace engine + schema diffs (`src/runw/trace.py`, `POST /api/pipeline/trace`)
- [ ] Execution trace / data lineage — Phase B+: row identity tracking, join/agg info, column provenance, expression gen, frontend panel, compare mode
- [ ] Rating table hot-reload with impact preview
- [ ] Natural language → Polars code (LLM-powered node assistant)

### Phase 5 — Engineering Practices
- [ ] `runw init` scaffolds a new pricing project with CI/CD
- [ ] GitHub Actions template (lint → test → deploy on merge)
- [ ] Pre-commit hooks (ruff, mypy, runw lint)
- [ ] Auto-generated test stubs + `runw test`
- [ ] `runw lint` pipeline-specific validation
- [ ] Pipeline visual diff (`runw diff HEAD~1`)

### Phase 6 — Advanced
- [ ] Composable pipelines (sub-pipelines as nodes)
- [ ] Monitoring dashboard (actual vs expected pricing)
- [ ] A/B testing for deployed endpoints
- [ ] Optimisation engine
- [ ] Collaborative editing (multi-user)
