# Haute Deploy - Design & Implementation Plan

**Status:** Design  
**Scope:** Phase 3 - Deploy & Score  

---

## 1. Problem Statement

A pricing analyst has built a pipeline in Haute (e.g. `my_pipeline.py`). They need to deploy it as a **live API** so that policy admin systems can send a JSON quote request and receive a priced response - with one command, no DevOps, no manual packaging.

The full pipeline typically contains branches that are irrelevant for live scoring (training data joins, data exports, exploratory sinks). The deployment must **prune** to only the scoring path: input → models → premium → output.

### Current pipeline example

```
claims ──→ claims_aggregate ─┐
                              ├─→ frequency_set → frequency_write   (data prep - NOT deployed)
exposure ─────────────────────┘
                              ┌─→ severity_set  → severity_write    (data prep - NOT deployed)
exposure ─────────────────────┘
claims ───────────────────────┘

policies ──→ frequency_model ──→ calculate_premium ──→ output       ← DEPLOYED
policies ──→ severity_model  ──┘                                    ← DEPLOYED
```

Only the **ancestors of the output node** are deployed. The `policies` source node becomes the live JSON input.

---

## 2. Competitive Advantages Over WTW Radar

Radar's deployment weaknesses (from research report §7):

| Radar Weakness | Haute Advantage |
|---|---|
| **Vendor lock-in** - models not portable, proprietary format | Pipeline is a `.py` file. Deployment is an MLflow model. Both are open standards. Zero lock-in. |
| **Opaque pricing** - $100K–$1M+/year, no public pricing | Free and open source. MLflow Model Serving cost = cloud compute only. |
| **Azure-only** - SaaS locked to Microsoft Azure | MLflow runs on Databricks (any cloud), local, or self-hosted. Future targets: SageMaker, GCP Vertex, Docker. |
| **Proprietary tooling** - Radar-specific skills not transferable | Pure Python + Polars. Skills are portable to any data science role. |
| **No public docs** - no Stack Overflow, no GitHub, no community | Open-source, public docs, community contributions. |
| **Implementation complexity** - requires WTW consulting engagement | `haute deploy` - one command. No consultants required. |
| **Learning curve** - Radar's own modelling language | Standard `@pipeline.node` decorated Python functions. |
| **No version control native** - audit trail is platform-managed | Git-native. Every deploy is a git commit + MLflow model version. Full diff, rollback, PR review. |

### Haute-specific advantages

1. **Identical code for dev and prod** - `pipeline.score(df)` locally is the same code path as the live API. No "export to ONNX/PMML" lossy translation.
2. **Pre-deploy dry-run** - score a sample row locally before deploying. Catch errors before they reach production.
3. **Auto-generated input/output schema** - MLflow model signature derived from the pipeline's actual data. No manual schema definition.
4. **Graph pruning** - only the scoring path is deployed. No dead code in production.
5. **Artifact auto-bundling** - model files (`.cbm`, `.pkl`, etc.) discovered and packaged automatically from the graph.
6. **Deployment manifest** - machine-readable JSON describing exactly what was deployed, when, by whom, and what the input/output contract is.
7. **Local-first** - works without Databricks. Deploy to local MLflow for testing, Databricks for production. Same command.

---

## 3. User Experience

### 3.1 Project setup (`haute init`)

`haute init` scaffolds everything needed for deployment:

```
my_project/
  haute.toml              ← project & deploy config (committed to git)
  .env.example           ← Databricks credentials template (committed)
  .env                   ← actual secrets (gitignored)
  main.py               ← starter pipeline
  data/                  ← input data files
  tests/                 ← Tests + JSON payloads for pre-deploy testing
    quotes/
      example.json
  .gitignore             ← includes .env
```

### 3.2 Configuration: `haute.toml` (implemented)

All deploy settings live in a single TOML file at the project root.
Created by `haute init`, committed to git, shared by the team.

```toml
[project]
name = "my_pipeline"
pipeline = "main.py"

[deploy]
target = "databricks"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.databricks]
experiment_name = "/Shared/hauteay/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"
serving_scale_to_zero = true

[test_quotes]
dir = "tests/quotes"
```

**What lives where:**

| Setting | Location | Committed? |
|---|---|---|
| Pipeline file, model name, endpoint | `haute.toml` | Yes |
| Databricks experiment, catalog, schema | `haute.toml` | Yes |
| Serving size, scale-to-zero | `haute.toml` | Yes |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | `.env` | **No** (gitignored) |
| Test quote JSON payloads | `tests/quotes/` | Yes |

### 3.3 Secrets: `.env` (implemented)

Databricks credentials are loaded from `.env` at deploy time.
A `.env.example` template is created by `haute init`:

```bash
# .env.example - copy to .env and fill in
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net
DATABRICKS_TOKEN=your_databricks_token_here
```

Setup is one command:
```bash
cp .env.example .env
# edit .env with your actual credentials
```

### 3.4 Test quotes (implemented)

The `tests/quotes/` directory contains JSON files that are run through the
pipeline during pre-deploy validation. Each file is a JSON array of quote
objects matching the input schema:

```
tests/quotes/
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

During `haute deploy`, **every** JSON file in `tests/quotes/` is scored
through the pruned pipeline. If any file fails, deployment is blocked
with a clear error. This catches:
- Schema mismatches (missing/wrong columns)
- Runtime errors in transform code
- Model loading failures
- Edge case crashes (nulls, extreme values)

### 3.5 One-command deploy

```bash
# Reads everything from haute.toml + .env
haute deploy

# Or override specific settings
haute deploy --model-name motor-pricing-v2 --dry-run
```

Output:
```
Deploying pipeline: my_pipeline
  ✓ Loaded config from haute.toml
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
# In the pipeline code - policies is the live API input
@pipeline.node(path="data/policies.parquet", deploy_input=True)
def policies() -> pl.DataFrame:
    return pl.scan_parquet("data/policies.parquet")

# Other sources (claims, exposure) are NOT deploy inputs -
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
src/haute/
  deploy/
    __init__.py             # Public API: deploy(), DeployConfig, HauteModel
    _config.py              # DeployConfig dataclass, auto-detection, haute.toml loading
    _pruner.py              # Graph pruning to output ancestors
    _bundler.py             # Artifact discovery and collection
    _scorer.py              # score_graph() - the runtime scoring engine for deployed models
    _model.py               # HauteModel(mlflow.pyfunc.PythonModel)
    _schema.py              # Input/output schema inference
    _validators.py          # Pre-deploy validation (dry-run, artifact checks)
    _mlflow.py              # deploy_to_mlflow(), DeployResult, get_deploy_status()
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
1. Compute `ancestors(output_node_id, edges, all_ids)` - already exists
2. Filter nodes and edges to only the ancestor set
3. Identify source nodes (nodes with no incoming edges in the pruned graph)
4. Return the pruned graph + metadata

### 4.3 Deploy configuration (`_config.py`)

User-provided config (from `haute.toml` + CLI overrides) is strictly separated
from computed state (pruned graph, schemas, artifacts). This avoids mixing
input and output in the same dataclass (commit standards §3 Single Responsibility).

```python
@dataclass
class DatabricksConfig:
    """Typed Databricks-specific settings from [deploy.databricks] in haute.toml."""
    experiment_name: str = "/Shared/hauteay/default"
    catalog: str = "main"
    schema: str = "pricing"
    serving_workload_size: str = "Small"
    serving_scale_to_zero: bool = True


@dataclass
class DeployConfig:
    """User-provided deployment configuration (from haute.toml + CLI)."""
    pipeline_file: Path
    model_name: str                                   # MLflow registered model name
    endpoint_name: str | None = None                  # Databricks serving endpoint
    output_fields: list[str] | None = None            # Limit output columns
    test_quotes_dir: Path | None = None               # Directory of test JSON files
    databricks: DatabricksConfig = field(default_factory=DatabricksConfig)

    @classmethod
    def from_toml(cls, path: Path) -> DeployConfig:
        """Load from haute.toml, merging [project] and [deploy] sections."""

    def override(self, **cli_kwargs: str | None) -> DeployConfig:
        """Return a copy with non-None CLI flags applied over TOML values."""


@dataclass
class ResolvedDeploy:
    """Computed state after resolving a DeployConfig against a parsed pipeline.

    Created by resolve_config() - never constructed directly.
    """
    config: DeployConfig
    pruned_graph: dict[str, list[dict[str, str]]]     # React Flow graph JSON
    input_node_ids: list[str]                          # deploy_input=True sources
    output_node_id: str                                # output=True node
    artifacts: dict[str, Path]                         # artifact_name → absolute_path
    input_schema: dict[str, str]                       # column_name → polars dtype
    output_schema: dict[str, str]                      # column_name → polars dtype


def resolve_config(config: DeployConfig) -> ResolvedDeploy:
    """Parse pipeline, prune graph, detect I/O nodes, collect artifacts, infer schemas."""
```

**Auto-detection rules:**
- **`output_node`**: Find the node with `config.output=True` or `nodeType="output"`. If exactly one exists, use it. Otherwise, error with a clear message.
- **`input_node`**: After pruning, find source nodes with `config.deploy_input=True`. If none are marked, fall back to the single source node in the pruned graph. If multiple unmarked sources exist, error.
- **`model_name`**: From `haute.toml [deploy].model_name`, falling back to `pipeline.name` (sanitized for MLflow).

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
| `dataSource` | NOT the input node | The data file (`config.path`) - becomes a static lookup table |
| `dataSource` | IS the input node | **Not bundled** - replaced by live JSON input |

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

The input schema is read from the actual data file that the input source node points to. This becomes the MLflow model signature - callers know exactly what fields to send.

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
class HauteModel(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel wrapper for a deployed haute pipeline.

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
        from haute.deploy._scorer import score_graph

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
    "haute_version": "0.1.0",
    "pipeline_name": "my_pipeline",
    "pipeline_file": "main.py",
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
        "freq_model": "models/freq.cbm",
        "sev_model": "models/sev.cbm"
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

### 4.9 MLflow deploy (`_mlflow.py`)

v1 has exactly one deployment target: MLflow (Databricks-backed). Per commit
standards §"Over-abstraction": no `Protocol`, no base class, no factory - just
a plain function. When a second target is needed, *that's* when we extract the
common interface.

```python
@dataclass
class DeployResult:
    """Returned by deploy_to_mlflow()."""
    model_name: str
    model_version: int
    model_uri: str               # e.g. "models:/main.pricing.motor-pricing/3"
    endpoint_url: str | None     # Databricks serving URL, if created
    manifest_path: Path

def deploy_to_mlflow(resolved: ResolvedDeploy) -> DeployResult:
    """Deploy a resolved pipeline to MLflow + Databricks Model Serving.

    Steps:
        1. Set tracking URI (``databricks``) and registry URI (``databricks-uc``)
        2. Build Unity Catalog model name (``catalog.schema.model_name``)
        3. Ensure experiment parent directories exist in the workspace
        4. Build deployment manifest JSON
        5. Log HauteModel via models-from-code (file path, not CloudPickle) with artifacts + signature
        6. Register model version in MLflow Model Registry
        7. Create or update Databricks Model Serving endpoint (if endpoint_name is set)
        8. Return DeployResult with model URI and endpoint URL
    """

def get_deploy_status(
    model_name: str, catalog: str = "main", schema: str = "default"
) -> dict[str, str | int]:
    """Query MLflow Model Registry for current model versions and serving status.

    Sets tracking/registry URIs and constructs the UC three-level model name.
    """
```

**Why no abstraction layer:** The pruner, bundler, schema inference, scorer,
validator, and manifest are all target-agnostic already. Only the final
"log model + create endpoint" step is MLflow-specific. When a second target
arrives (e.g. Docker, SageMaker), the refactor is small: extract that one
function into a dispatch, not rebuild a class hierarchy. See §8 for the
extensibility plan.

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
@click.argument("pipeline_file", required=False)
@click.option("--model-name", default=None, help="Override model name from haute.toml")
@click.option("--dry-run", is_flag=True, help="Validate and score test quotes without deploying")
def deploy(pipeline_file: str | None, model_name: str | None, dry_run: bool) -> None:
    """Deploy a pipeline as a live scoring API.

    Reads config from haute.toml + credentials from .env.
    Pipeline file, model name, and target are all optional -
    defaults come from [project] and [deploy] in haute.toml.
    """
```

**Resolution order for settings:**
1. CLI flags (highest priority)
2. `haute.toml` `[deploy]` section
3. Auto-detection from the pipeline (input/output nodes)

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
│  HauteModel.predict(model_input: pd.DataFrame)                  │
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

### Step 1: `_pruner.py` - Graph pruning
- Reuse `ancestors()` from `graph_utils.py`
- Filter graph to ancestor set of output node
- Identify source/output nodes in pruned graph
- **Tests:** prune the `my_pipeline` example, verify only 5 nodes remain

### Step 2: `_config.py` - Deploy configuration
- `DeployConfig` dataclass with auto-detection logic
- `resolve()` method that parses pipeline, prunes graph, detects I/O nodes
- `haute.toml` loading + `.env` credential loading
- **Tests:** auto-detection on `my_pipeline`, explicit overrides

### Step 3: `_bundler.py` - Artifact collection
- Walk pruned graph, collect model files and static data
- Generate artifact name → path mapping
- **Tests:** verify `freq.cbm` and `sev.cbm` collected for `my_pipeline`

### Step 4: `_schema.py` - Schema inference
- Read input source data to get column names + types
- Dry-run to get output schema
- Convert to MLflow `ModelSignature`
- **Tests:** verify schema matches actual pipeline data

### Step 5: `_scorer.py` - Runtime scoring engine
- Modified `_build_node_fn` that injects input DataFrame at source nodes
- Artifact path remapping for externalFile/dataSource nodes
- Returns collected output DataFrame
- **Tests:** score 1 row and N rows, compare output to `Pipeline.score()`

### Step 6: `_model.py` - MLflow PythonModel
- `HauteModel` class with `load_context` / `predict`
- Manifest loading and artifact path resolution
- **Tests:** save and load model, verify predict output matches local scoring

### Step 7: `_validators.py` - Pre-deploy validation
- All checks from §4.10
- Clear error messages for each failure mode
- **Tests:** intentionally break things and verify error messages

### Step 8: `_mlflow.py` - MLflow deploy function
- `deploy_to_mlflow(resolved)`: create manifest, log model, register, create endpoint
- `get_deploy_status(model_name)`: query model versions and serving status
- **Tests:** end-to-end deploy to local MLflow tracking server

### Step 9: CLI integration
- `haute deploy` command in `cli.py`
- `haute status` command for checking deployed endpoints
- Rich console output with progress and summary
- **Tests:** CLI integration tests

### Step 10: `pyproject.toml` updates
- Add `mlflow` to `[project.optional-dependencies].databricks`
- Ensure `haute[databricks]` installs everything needed for deploy

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

> **Documented exception to "Polars-native, never convert to pandas"**
> (commit standards - Design Philosophy §"Polars-native, lazy by default")

MLflow's `PythonModel.predict()` contract **requires** pandas DataFrames for
input and output. This is not optional - it's the MLflow serving protocol.

We convert at the outermost boundary only:
- `pd.DataFrame → pl.from_pandas() → score_graph() → result.to_pandas()`
- All internal computation remains Polars `LazyFrame` throughout
- The conversion happens in exactly one place: `HauteModel.predict()`
- No pandas is used inside the pipeline graph execution

If MLflow adds native Polars support in the future, this bridge is a
single-line removal.

### 7.4 Manifest-driven (not code-driven) deployment

The deployment manifest (`deploy_manifest.json`) is the single source of truth for a deployed model. It contains:
- The full pruned graph
- All configuration
- Input/output schemas
- Artifact paths
- Provenance metadata

This makes deployments **inspectable, reproducible, and debuggable**. You can look at the manifest and understand exactly what a deployed model does.

---

## 8. Extensibility - Adding Future Deploy Targets

### 8.1 What's already target-agnostic

The deploy pipeline is deliberately layered so that most modules don't know
or care where the model ends up:

```
                          target-agnostic
                    ┌─────────────────────────┐
  haute.toml ──→ DeployConfig ──→ resolve_config()
                                      │
                    _pruner.py        prune graph
                    _bundler.py       collect artifacts
                    _schema.py        infer schemas
                    _validators.py    run test quotes
                    _scorer.py        dry-run scoring
                                      │
                                      ▼
                              ResolvedDeploy
                    └─────────────────────────┘
                                      │
                          target-specific (v1: MLflow only)
                    ┌─────────────────────────┐
                    _mlflow.py        deploy_to_mlflow()
                    _model.py         HauteModel (PythonModel)
                    └─────────────────────────┘
```

The `ResolvedDeploy` dataclass is the clean handoff point. A new target only
needs to consume `ResolvedDeploy` and do its own packaging.

### 8.2 How to add a second target (when needed)

When a second target arrives, the refactor is:

1. **Add `[deploy] target = "docker"` to `haute.toml`** - the config already
   has a `target` field
2. **Write `_docker.py`** with a `deploy_to_docker(resolved: ResolvedDeploy) -> DeployResult` function
3. **Add dispatch in `deploy/__init__.py`:**
   ```python
   def deploy(config: DeployConfig) -> DeployResult:
       resolved = resolve_config(config)
       if config.target == "databricks":
           return deploy_to_mlflow(resolved)
       elif config.target == "docker":
           return deploy_to_docker(resolved)
       raise ValueError(f"Unknown target: {config.target}")
   ```
4. **Extract a `DeployTarget` Protocol *only if* three targets exist** - at that
   point the pattern is proven and the interface is obvious from the concrete
   implementations

This is a small, safe refactor because the target-agnostic layers don't change.

### 8.3 Planned future targets

| Target | `[deploy] target =` | What it produces | When |
|---|---|---|---|
| **Databricks MLflow** | `"databricks"` | MLflow model + serving endpoint | **v1 (now)** |
| **Docker** | `"docker"` | Dockerfile + FastAPI app + `docker-compose.yml` | v2 |
| **Databricks batch job** | `"databricks-batch"` | Databricks Job running `score_graph()` on a schedule | v2 |
| **AWS SageMaker** | `"sagemaker"` | SageMaker endpoint via `sagemaker-sdk` | v3 |
| **GCP Vertex AI** | `"vertex"` | Vertex endpoint | v3 |
| **Standalone FastAPI** | `"fastapi"` | Self-contained Python package with FastAPI server | v3 |

### 8.4 Other future features (not in v1)

| Feature | Description |
|---|---|
| **Deploy from GUI** | "Deploy" button in the React Flow UI |
| **Deploy diff** | Compare two deployed model versions |
| **Canary/shadow deploy** | Route % of traffic to new model version |
| **A/B testing** | Traffic splitting between model versions with metric tracking |
| **Rollback** | `haute rollback motor-pricing` → revert to previous model version |
| **Deploy hooks** | Pre/post-deploy scripts (e.g., notify Slack, run integration tests) |
| **Input validation** | Runtime schema validation with clear error messages for missing/wrong-type fields |
| **Response caching** | LRU cache for repeated identical inputs |
| **Monitoring** | Log predictions to MLflow for drift detection |

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
| `deploy/__init__.py` | ~30 | Public exports: `deploy()`, `DeployConfig`, `HauteModel` |
| `deploy/_pruner.py` | ~60 | `prune_for_deploy()` - graph pruning |
| `deploy/_config.py` | ~120 | `DeployConfig`, `DatabricksConfig`, `ResolvedDeploy`, `resolve_config()` |
| `deploy/_bundler.py` | ~80 | `collect_artifacts()` - discover and collect model/data files |
| `deploy/_schema.py` | ~80 | `infer_input_schema()`, `infer_output_schema()`, MLflow signature |
| `deploy/_scorer.py` | ~100 | `score_graph()` - runtime scoring with input injection |
| `deploy/_model.py` | ~90 | `HauteModel(PythonModel)` - MLflow wrapper |
| `deploy/_validators.py` | ~100 | `validate_deploy()` - pre-deploy checks |
| `deploy/_mlflow.py` | ~120 | `deploy_to_mlflow()`, `DeployResult`, `get_deploy_status()` |
| `cli.py` (additions) | ~80 | `haute deploy` + `haute status` commands |
| **Total** | **~860** | |
