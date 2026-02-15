# Runway Deploy — Design & Implementation Plan

**Status:** Design  
**Scope:** Phase 3 — Deploy & Score  

---

## 1. Problem Statement

A pricing analyst has built a pipeline in Runway (e.g. `my_pipeline.py`). They need to deploy it as a **live API** so that policy admin systems can send a JSON quote request and receive a priced response — with one command, no DevOps, no manual packaging.

The full pipeline typically contains branches that are irrelevant for live scoring (training data joins, data exports, exploratory sinks). The deployment must **prune** to only the scoring path: input → models → premium → output.

### Current pipeline example

```
claims ──→ claims_aggregate ─┐
                              ├─→ frequency_set → frequency_write   (data prep — NOT deployed)
exposure ─────────────────────┘
                              ┌─→ severity_set  → severity_write    (data prep — NOT deployed)
exposure ─────────────────────┘
claims ───────────────────────┘

policies ──→ frequency_model ──→ calculate_premium ──→ output       ← DEPLOYED
policies ──→ severity_model  ──┘                                    ← DEPLOYED
```

Only the **ancestors of the output node** are deployed. The `policies` source node becomes the live JSON input.

---

## 2. Competitive Advantages Over WTW Radar

Radar's deployment weaknesses (from research report §7):

| Radar Weakness | Runway Advantage |
|---|---|
| **Vendor lock-in** — models not portable, proprietary format | Pipeline is a `.py` file. Deployment is an MLflow model. Both are open standards. Zero lock-in. |
| **Opaque pricing** — $100K–$1M+/year, no public pricing | Free and open source. MLflow Model Serving cost = cloud compute only. |
| **Azure-only** — SaaS locked to Microsoft Azure | MLflow runs on Databricks (any cloud), local, or self-hosted. Future targets: SageMaker, GCP Vertex, Docker. |
| **Proprietary tooling** — Radar-specific skills not transferable | Pure Python + Polars. Skills are portable to any data science role. |
| **No public docs** — no Stack Overflow, no GitHub, no community | Open-source, public docs, community contributions. |
| **Implementation complexity** — requires WTW consulting engagement | `runw deploy` — one command. No consultants required. |
| **Learning curve** — Radar's own modelling language | Standard `@pipeline.node` decorated Python functions. |
| **No version control native** — audit trail is platform-managed | Git-native. Every deploy is a git commit + MLflow model version. Full diff, rollback, PR review. |

### Runway-specific advantages

1. **Identical code for dev and prod** — `pipeline.score(df)` locally is the same code path as the live API. No "export to ONNX/PMML" lossy translation.
2. **Pre-deploy dry-run** — score a sample row locally before deploying. Catch errors before they reach production.
3. **Auto-generated input/output schema** — MLflow model signature derived from the pipeline's actual data. No manual schema definition.
4. **Graph pruning** — only the scoring path is deployed. No dead code in production.
5. **Artifact auto-bundling** — model files (`.cbm`, `.pkl`, etc.) discovered and packaged automatically from the graph.
6. **Deployment manifest** — machine-readable JSON describing exactly what was deployed, when, by whom, and what the input/output contract is.
7. **Local-first** — works without Databricks. Deploy to local MLflow for testing, Databricks for production. Same command.

---

## 3. User Experience

### 3.1 Project setup (`runw init`)

`runw init` scaffolds everything needed for deployment:

```
my_project/
  runw.toml              ← project & deploy config (committed to git)
  .env.example           ← Databricks credentials template (committed)
  .env                   ← actual secrets (gitignored)
  pipelines/main.py      ← starter pipeline
  data/                  ← input data files
  test_quotes/           ← JSON payloads for pre-deploy testing
    example.json
  .gitignore             ← includes .env
```

### 3.2 Configuration: `runw.toml` (implemented)

All deploy settings live in a single TOML file at the project root.
Created by `runw init`, committed to git, shared by the team.

```toml
[project]
name = "my_pipeline"
pipeline = "pipelines/my_pipeline.py"

[deploy]
target = "databricks"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.databricks]
experiment_name = "/Shared/runway/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"
serving_scale_to_zero = true

[test_quotes]
dir = "test_quotes"
```

**What lives where:**

| Setting | Location | Committed? |
|---|---|---|
| Pipeline file, model name, endpoint | `runw.toml` | Yes |
| Databricks experiment, catalog, schema | `runw.toml` | Yes |
| Serving size, scale-to-zero | `runw.toml` | Yes |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | `.env` | **No** (gitignored) |
| Test quote JSON payloads | `test_quotes/` | Yes |

### 3.3 Secrets: `.env` (implemented)

Databricks credentials are loaded from `.env` at deploy time.
A `.env.example` template is created by `runw init`:

```bash
# .env.example — copy to .env and fill in
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net
DATABRICKS_TOKEN=your_databricks_token_here
```

Setup is one command:
```bash
cp .env.example .env
# edit .env with your actual credentials
```

### 3.4 Test quotes (implemented)

The `test_quotes/` directory contains JSON files that are run through the
pipeline during pre-deploy validation. Each file is a JSON array of quote
objects matching the input schema:

```
test_quotes/
  single_policy.json     ← 1 quote, quick smoke test
  batch_policies.json    ← 5 quotes, variety of risk profiles
  edge_cases.json        ← extreme values (young driver, old vehicle, etc.)
```

Example `single_policy.json`:
```json
[
  {
    "IDpol": 99001,
    "VehPower": 7,
    "VehAge": 3,
    "DrivAge": 42,
    "BonusMalus": 50,
    "VehBrand": "B12",
    "VehGas": "Diesel",
    "Area": "C",
    "Density": 850,
    "Region": "Ile-de-France"
  }
]
```

During `runw deploy`, **every** JSON file in `test_quotes/` is scored
through the pruned pipeline. If any file fails, deployment is blocked
with a clear error. This catches:
- Schema mismatches (missing/wrong columns)
- Runtime errors in transform code
- Model loading failures
- Edge case crashes (nulls, extreme values)

### 3.5 One-command deploy

```bash
# Reads everything from runw.toml + .env
runw deploy

# Or override specific settings
runw deploy --model-name motor-pricing-v2 --dry-run
```

Output:
```
Deploying pipeline: my_pipeline
  ✓ Loaded config from runw.toml
  ✓ Loaded credentials from .env
  ✓ Parsed pipeline (12 nodes, 14 edges)
  ✓ Pruned to output ancestors (5 nodes)
  ✓ Collected 2 artifacts (freq.cbm, sev.cbm)
  ✓ Inferred input schema (10 columns from policies)
  ✓ Test quotes: single_policy.json ............ 1 row   ✓  (18ms)
  ✓ Test quotes: batch_policies.json ........... 5 rows  ✓  (24ms)
  ✓ Test quotes: edge_cases.json ............... 3 rows  ✓  (19ms)
  ✓ Logged MLflow model: motor-pricing v3
  ✓ Deployed to endpoint: motor-pricing (Databricks)

Endpoint ready:
  POST https://adb-xxxxx.azuredatabricks.net/serving-endpoints/motor-pricing/invocations
```

### 3.6 Marking the live API input (implemented)

The `deploy_input=True` decorator flag marks which data source receives live JSON requests.
This is now built into the core pipeline structure and works across the full stack:

```python
# In the pipeline code — policies is the live API input
@pipeline.node(path="data/policies.parquet", deploy_input=True)
def policies() -> pl.DataFrame:
    return pl.scan_parquet("data/policies.parquet")

# Other sources (claims, exposure) are NOT deploy inputs —
# they either get pruned or bundled as static artifacts.

@pipeline.node(output=True)
def output(calculate_premium: pl.DataFrame) -> pl.DataFrame:
    return calculate_premium
```

**How it flows through the system:**

| Layer | What happens |
|---|---|
| **Decorator** | `deploy_input=True` stored in `Node.config` |
| **Parser** | AST extraction preserves the kwarg in `config` |
| **Codegen** | Templates emit `deploy_input=True` when present (round-trip safe) |
| **Pipeline.score()** | Only seeds `deploy_input` sources with the live DataFrame; other sources run their own load logic |
| **GUI (node)** | Green "API" badge on the node card |
| **GUI (panel)** | Toggle button in DataSource config panel |
| **Deploy** | Auto-detects input node from `deploy_input=True`; no CLI flag needed |

**Fallback:** If no source is marked `deploy_input`, `Pipeline.score()` seeds
all sources (backward-compatible). Deploy auto-detection falls back to the
single source node in the pruned graph.

### 3.7 Live scoring after deployment

```bash
# Via MLflow serving
curl -X POST http://localhost:5001/invocations \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"Area": "A", "VehPower": 5, "VehAge": 3}]}'
# → [{"freq_preds": 0.12, "sev_preds": 3200.0, "technical_price": 384.0, "premium": 548.57}]

# Via Databricks Model Serving
curl -X POST https://<workspace>.databricks.net/serving-endpoints/motor-pricing/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"dataframe_records": [{"Area": "A", "VehPower": 5, "VehAge": 3}]}'
```

---

## 4. Technical Design

### 4.1 Module structure

```
src/runw/
  deploy/
    __init__.py             # Public API: deploy(), DeployConfig, RunwayModel
    _config.py              # DeployConfig dataclass, auto-detection, YAML loading
    _pruner.py              # Graph pruning to output ancestors
    _bundler.py             # Artifact discovery and collection
    _scorer.py              # score_graph() — the runtime scoring engine for deployed models
    _model.py               # RunwayModel(mlflow.pyfunc.PythonModel)
    _schema.py              # Input/output schema inference
    _validators.py          # Pre-deploy validation (dry-run, artifact checks)
    _targets.py             # DeployTarget protocol + MLflow implementation
```

### 4.2 Graph pruning (`_pruner.py`)

Reuses the existing `ancestors()` function from `graph_utils.py`.

```python
def prune_for_deploy(
    graph: dict,
    output_node_id: str,
) -> tuple[dict, list[str], list[str]]:
    """Prune a graph to only the ancestors of the output node.

    Returns:
        (pruned_graph, source_node_ids, removed_node_ids)
    """
```

**Logic:**
1. Compute `ancestors(output_node_id, edges, all_ids)` — already exists
2. Filter nodes and edges to only the ancestor set
3. Identify source nodes (nodes with no incoming edges in the pruned graph)
4. Return the pruned graph + metadata

### 4.3 Deploy configuration (`_config.py`)

```python
@dataclass
class DeployConfig:
    """Configuration for deploying a pipeline."""

    pipeline_file: Path
    model_name: str                         # MLflow model registry name
    input_node: str | None = None           # Auto-detected if None
    output_node: str | None = None          # Auto-detected if None
    output_fields: list[str] | None = None  # Limit output columns
    target: str = "mlflow"                  # "mlflow" | "databricks" (future)
    target_config: dict = field(default_factory=dict)

    # Auto-populated during resolve()
    pruned_graph: dict = field(default_factory=dict, repr=False)
    artifacts: dict[str, Path] = field(default_factory=dict)
    input_schema: dict[str, str] = field(default_factory=dict)
    output_schema: dict[str, str] = field(default_factory=dict)
```

**Auto-detection rules:**
- `output_node`: Find the node with `config.output=True` or `nodeType="output"`. If exactly one exists, use it. Otherwise, require explicit specification.
- `input_node`: After pruning, find all source nodes (`nodeType="dataSource"`). If exactly one exists, use it as the live input. If multiple exist, require explicit specification (others become static artifacts).
- `model_name`: Default to `pipeline.name` (sanitized for MLflow).

### 4.4 Artifact bundling (`_bundler.py`)

Walks the pruned graph and collects files that need to be packaged:

```python
def collect_artifacts(
    pruned_graph: dict,
    input_node_id: str,
    pipeline_dir: Path,
) -> dict[str, Path]:
    """Discover and collect all artifacts needed for deployment.

    Returns:
        Dict of artifact_name → absolute_path for:
        - externalFile nodes: model files (.cbm, .pkl, .joblib, etc.)
        - dataSource nodes (non-input): static data files bundled as artifacts
    """
```

**What gets bundled:**
| Node type | Condition | Artifact |
|---|---|---|
| `externalFile` | Always | The model file (`config.path`) |
| `dataSource` | NOT the input node | The data file (`config.path`) — becomes a static lookup table |
| `dataSource` | IS the input node | **Not bundled** — replaced by live JSON input |

### 4.5 Schema inference (`_schema.py`)

```python
def infer_input_schema(
    graph: dict,
    input_node_id: str,
) -> dict[str, str]:
    """Infer the input schema by executing the input source node and reading its columns.

    Returns:
        Dict of column_name → polars_dtype_string
    """

def infer_output_schema(
    graph: dict,
    output_node_id: str,
    sample_input: pl.DataFrame,
) -> dict[str, str]:
    """Infer the output schema by running a sample row through the pruned graph.

    Returns:
        Dict of column_name → polars_dtype_string
    """
```

The input schema is read from the actual data file that the input source node points to. This becomes the MLflow model signature — callers know exactly what fields to send.

### 4.6 Scoring engine (`_scorer.py`)

This is the **runtime** scoring function that executes inside the deployed model. It must be self-contained and have no dependency on the filesystem layout of the development environment.

```python
def score_graph(
    graph: dict,
    input_df: pl.DataFrame,
    input_node_ids: list[str],
    output_node_id: str,
    artifact_paths: dict[str, str] | None = None,
    output_fields: list[str] | None = None,
) -> pl.DataFrame:
    """Execute a pruned pipeline graph with injected input data.

    Instead of loading from files, input source nodes receive the provided
    DataFrame. Artifact paths are remapped to the MLflow artifact directory.

    Args:
        graph: Pruned React Flow graph JSON.
        input_df: The live input data (1 or N rows).
        input_node_ids: Source node IDs that receive the live input.
        output_node_id: The node whose output is the API response.
        artifact_paths: Remapped artifact paths (artifact_name → local_path).
        output_fields: Optional list of columns to select from output.

    Returns:
        Output DataFrame (1 or N rows).
    """
```

**Key difference from `execute_graph()`:**
- Source nodes marked as `input_node` inject the provided DataFrame instead of reading from disk
- Artifact paths in `externalFile` and static `dataSource` nodes are remapped to MLflow artifact locations
- Only executes up to `output_node_id` (graph is already pruned, but we pass `target_node_id` anyway)
- Returns a single collected DataFrame, not per-node preview dicts

**Implementation approach:**
Uses a modified `_build_node_fn` that intercepts source/external nodes and redirects their paths. The existing `_execute_lazy()` infrastructure handles the rest.

### 4.7 MLflow PythonModel (`_model.py`)

```python
class RunwayModel(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel wrapper for a deployed runw pipeline.

    Artifacts:
        - deploy_manifest.json: Deployment manifest with graph, config, schemas
        - [model files]: .cbm, .pkl, .joblib, etc.
        - [static data]: .parquet, .csv for non-input data sources
    """

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        """Called once when the model is loaded for serving.

        Loads the deployment manifest and resolves artifact paths.
        """
        manifest_path = Path(context.artifacts["deploy_manifest"])
        self._manifest = json.loads(manifest_path.read_text())
        self._graph = self._manifest["graph"]
        self._input_node_ids = self._manifest["input_nodes"]
        self._output_node_id = self._manifest["output_node"]
        self._output_fields = self._manifest.get("output_fields")

        # Remap artifact paths to MLflow artifact directory
        self._artifact_paths = {}
        for name, _original_path in self._manifest["artifacts"].items():
            self._artifact_paths[name] = context.artifacts[name]

        # Rewrite graph node configs to point to resolved artifact paths
        self._rewrite_artifact_paths()

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: pd.DataFrame,
        params: dict | None = None,
    ) -> pd.DataFrame:
        """Score one or more rows through the pipeline.

        Input: pandas DataFrame (MLflow convention)
        Output: pandas DataFrame
        """
        import polars as pl
        from runw.deploy._scorer import score_graph

        input_df = pl.from_pandas(model_input)

        result = score_graph(
            graph=self._graph,
            input_df=input_df,
            input_node_ids=self._input_node_ids,
            output_node_id=self._output_node_id,
            output_fields=self._output_fields,
        )

        return result.to_pandas()
```

### 4.8 Deployment manifest (`deploy_manifest.json`)

Written at deploy time, bundled as an MLflow artifact:

```json
{
    "runw_version": "0.1.0",
    "pipeline_name": "my_pipeline",
    "pipeline_file": "pipelines/my_pipeline.py",
    "created_at": "2026-02-15T16:06:00Z",
    "created_by": "ralph",

    "input_nodes": ["policies"],
    "output_node": "output",
    "output_fields": null,

    "input_schema": {
        "IDpol": "Int64",
        "Area": "String",
        "VehPower": "Int64",
        "VehAge": "Int64",
        "DrivAge": "Int64",
        "BonusMalus": "Int64",
        "VehBrand": "String",
        "VehGas": "String",
        "Density": "Int64",
        "Region": "String"
    },
    "output_schema": {
        "IDpol": "Int64",
        "freq_preds": "Float64",
        "sev_preds": "Float64",
        "technical_price": "Float64",
        "premium": "Float64"
    },

    "artifacts": {
        "freq_model": "pipelines/models/freq.cbm",
        "sev_model": "pipelines/models/sev.cbm"
    },

    "graph": { "...pruned graph JSON..." },

    "nodes_deployed": 5,
    "nodes_skipped": 7,
    "nodes_skipped_names": [
        "claims", "exposure", "claims_aggregate",
        "frequency_set", "severity_set",
        "frequency_write", "severity_write"
    ]
}
```

### 4.9 Deploy targets (`_targets.py`)

```python
class DeployTarget(Protocol):
    """Protocol for deployment targets."""

    def deploy(self, config: DeployConfig) -> DeployResult: ...
    def status(self, model_name: str) -> dict: ...


class MLflowTarget:
    """Deploy to MLflow Model Registry (local or Databricks-backed)."""

    def deploy(self, config: DeployConfig) -> DeployResult:
        """
        1. Create deployment manifest
        2. Log RunwayModel as mlflow.pyfunc with artifacts
        3. Register in MLflow Model Registry
        4. Return model URI
        """

    def status(self, model_name: str) -> dict:
        """Check model versions and serving status."""


# Future targets (not implemented in v1):
# class DatabricksServingTarget — creates/updates a Model Serving endpoint
# class DockerTarget — builds a Docker image with the model
# class FastAPITarget — generates a standalone FastAPI app
```

### 4.10 Validation (`_validators.py`)

Pre-deploy checks run before any MLflow logging:

```python
def validate_deploy(config: DeployConfig) -> list[str]:
    """Run all pre-deploy validations. Returns list of errors (empty = ok)."""
```

| Check | Description |
|---|---|
| **Pipeline parseable** | File parses without syntax errors |
| **Output node exists** | The declared output node is in the graph |
| **Input node exists** | The declared input node is in the pruned graph |
| **Input node is a source** | It has no incoming edges |
| **All artifacts exist** | Every model file / static data file is on disk |
| **No unresolved nodes** | No `NotImplementedError` (e.g. Databricks source) in the pruned graph |
| **Dry-run passes** | Score 1 sample row through the pruned graph and confirm no runtime errors |
| **Output has rows** | The dry-run produces at least 1 output row |

### 4.11 CLI integration

```python
@cli.command()
@click.argument("pipeline_file")
@click.option("--model-name", default=None, help="MLflow model name (default: pipeline name)")
@click.option("--input-node", default=None, help="Source node for live input (auto-detected)")
@click.option("--output-node", default=None, help="Output node (auto-detected)")
@click.option("--target", default="mlflow", type=click.Choice(["mlflow"]), help="Deploy target")
@click.option("--dry-run", is_flag=True, help="Validate and dry-run without deploying")
def deploy(pipeline_file, model_name, input_node, output_node, target, dry_run):
    """Deploy a pipeline as a live scoring API."""
```

---

## 5. Data Flow (Runtime)

```
┌─────────────────────────────────────────────────────────────────┐
│  API Request                                                    │
│  POST /invocations                                              │
│  {"dataframe_records": [{"Area":"A","VehPower":5,"VehAge":3}]}  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  MLflow Model Serving / Databricks Endpoint                      │
│                                                                  │
│  RunwayModel.predict(model_input: pd.DataFrame)                  │
│  │                                                               │
│  │  1. pl.from_pandas(model_input)          → 1-row Polars DF   │
│  │  2. score_graph(graph, input_df, ...)    → pipeline exec      │
│  │     │                                                         │
│  │     │  policies (INPUT) ─→ inject live DataFrame              │
│  │     │  frequency_model  ─→ load freq.cbm from artifacts       │
│  │     │  severity_model   ─→ load sev.cbm from artifacts        │
│  │     │  calculate_premium ─→ execute transform code             │
│  │     │  output           ─→ select output fields               │
│  │     │                                                         │
│  │  3. result.to_pandas()                   → response DF        │
│  │                                                               │
└──┼───────────────────────────────────────────────────────────────┘
   │
   ▼
┌──────────────────────────────────────────────────────────────────┐
│  API Response                                                    │
│  {"predictions": [{"technical_price":384.0,"premium":548.57}]}   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 6. Implementation Order

### Step 1: `_pruner.py` — Graph pruning
- Reuse `ancestors()` from `graph_utils.py`
- Filter graph to ancestor set of output node
- Identify source/output nodes in pruned graph
- **Tests:** prune the `my_pipeline` example, verify only 5 nodes remain

### Step 2: `_config.py` — Deploy configuration
- `DeployConfig` dataclass with auto-detection logic
- `resolve()` method that parses pipeline, prunes graph, detects I/O nodes
- YAML sidecar loading (`*.deploy.yaml`)
- **Tests:** auto-detection on `my_pipeline`, explicit overrides

### Step 3: `_bundler.py` — Artifact collection
- Walk pruned graph, collect model files and static data
- Generate artifact name → path mapping
- **Tests:** verify `freq.cbm` and `sev.cbm` collected for `my_pipeline`

### Step 4: `_schema.py` — Schema inference
- Read input source data to get column names + types
- Dry-run to get output schema
- Convert to MLflow `ModelSignature`
- **Tests:** verify schema matches actual pipeline data

### Step 5: `_scorer.py` — Runtime scoring engine
- Modified `_build_node_fn` that injects input DataFrame at source nodes
- Artifact path remapping for externalFile/dataSource nodes
- Returns collected output DataFrame
- **Tests:** score 1 row and N rows, compare output to `Pipeline.score()`

### Step 6: `_model.py` — MLflow PythonModel
- `RunwayModel` class with `load_context` / `predict`
- Manifest loading and artifact path resolution
- **Tests:** save and load model, verify predict output matches local scoring

### Step 7: `_validators.py` — Pre-deploy validation
- All checks from §4.10
- Clear error messages for each failure mode
- **Tests:** intentionally break things and verify error messages

### Step 8: `_targets.py` — MLflow deploy target
- `MLflowTarget.deploy()`: create manifest, log model, register
- `MLflowTarget.status()`: query model versions
- **Tests:** end-to-end deploy to local MLflow tracking server

### Step 9: CLI integration
- `runw deploy` command in `cli.py`
- `runw status` command for checking deployed endpoints
- Rich console output with progress and summary
- **Tests:** CLI integration tests

### Step 10: `pyproject.toml` updates
- Add `mlflow` to `[project.optional-dependencies].databricks`
- Ensure `runw[databricks]` installs everything needed for deploy

---

## 7. Key Design Decisions

### 7.1 Graph JSON as the deployment unit (not the .py file)

The pruned graph JSON is what gets bundled and executed at serving time, not the `.py` source file. Reasons:
- The graph is already the execution format (`_execute_lazy()` operates on graph JSON)
- Graph is self-contained (no import resolution, no `sys.path` issues)
- Can be pruned, validated, and inspected as data
- The `.py` file is recorded in the manifest for provenance, but not executed at serving time

### 7.2 Modified `_build_node_fn` for scoring (not a new executor)

Rather than building a separate execution engine for deployment, we use the existing `_build_node_fn` / `_execute_lazy` infrastructure with a thin wrapper that:
- Intercepts `dataSource` nodes for the input node → returns the live DataFrame
- Rewrites `config.path` for `externalFile` and static `dataSource` nodes → points to artifact dir

This means **dev and prod run the exact same code**. The only difference is where data comes from.

### 7.3 pandas bridge for MLflow compatibility

MLflow's `PythonModel.predict()` receives/returns pandas DataFrames. We convert at the boundary:
- `pd.DataFrame → pl.from_pandas() → score_graph() → result.to_pandas()`
- This is a thin bridge — all internal computation is Polars

### 7.4 Manifest-driven (not code-driven) deployment

The deployment manifest (`deploy_manifest.json`) is the single source of truth for a deployed model. It contains:
- The full pruned graph
- All configuration
- Input/output schemas
- Artifact paths
- Provenance metadata

This makes deployments **inspectable, reproducible, and debuggable**. You can look at the manifest and understand exactly what a deployed model does.

---

## 8. Future Extensions (Not in v1)

| Feature | Description |
|---|---|
| **Databricks Model Serving target** | Auto-create/update serving endpoint via `databricks-sdk` |
| **Docker target** | Generate a Dockerfile + FastAPI wrapper for standalone deployment |
| **Canary/shadow deploy** | Route % of traffic to new model version |
| **Deploy from GUI** | "Deploy" button in the React Flow UI |
| **Deploy diff** | Compare two deployed model versions |
| **A/B testing** | Traffic splitting between model versions with metric tracking |
| **Rollback** | `runw rollback motor-pricing` → revert to previous model version |
| **Deploy hooks** | Pre/post-deploy scripts (e.g., notify Slack, run integration tests) |
| **Batch scoring job** | `runw deploy --mode batch` → Databricks job instead of serving endpoint |
| **Input validation** | Runtime schema validation with clear error messages for missing/wrong-type fields |
| **Response caching** | LRU cache for repeated identical inputs |
| **Monitoring integration** | Log predictions to MLflow for drift detection |

---

## 9. Dependencies

### New (added to `[project.optional-dependencies].databricks`)
- `mlflow >= 2.15.0` (already listed)
- `databricks-sdk >= 0.30.0` (already listed)

### No new core dependencies
The deploy module uses only:
- `polars` (already core)
- `mlflow` (already optional)
- Standard library: `json`, `pathlib`, `dataclasses`, `datetime`, `getpass`

---

## 10. File-by-File Summary

| File | Lines (est.) | Purpose |
|---|---|---|
| `deploy/__init__.py` | ~30 | Public exports: `deploy()`, `DeployConfig`, `RunwayModel` |
| `deploy/_pruner.py` | ~60 | `prune_for_deploy()` — graph pruning |
| `deploy/_config.py` | ~120 | `DeployConfig` dataclass, auto-detection, YAML loading |
| `deploy/_bundler.py` | ~80 | `collect_artifacts()` — discover and collect model/data files |
| `deploy/_schema.py` | ~80 | `infer_input_schema()`, `infer_output_schema()`, MLflow signature |
| `deploy/_scorer.py` | ~100 | `score_graph()` — runtime scoring with input injection |
| `deploy/_model.py` | ~90 | `RunwayModel(PythonModel)` — MLflow wrapper |
| `deploy/_validators.py` | ~100 | `validate_deploy()` — pre-deploy checks |
| `deploy/_targets.py` | ~120 | `DeployTarget` protocol + `MLflowTarget` implementation |
| `cli.py` (additions) | ~80 | `runw deploy` + `runw status` commands |
| **Total** | **~860** | |
