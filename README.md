# Runway

**Open-source pricing engine for insurance teams on Databricks.**

Build, visualise, and deploy pricing pipelines as Python code — with a browser-based GUI that stays in sync.

```bash
pip install runw
```

---

## What is Runway?

Runway gives insurance pricing teams a **code-first, GUI-friendly** way to build rating pipelines. Write standard Python with Polars, see it instantly in a visual editor, and deploy to a live API with one command.

- **Build** pipelines in code or the GUI — both stay in sync
- **Run** the same pipeline for 1-row live quotes and million-row batch jobs
- **Deploy** to Databricks MLflow Model Serving with `runw deploy`

Python code is always the source of truth. The GUI is a live, editable view.

---

## Quick Start

### 1. Install

```bash
pip install runw

# For deployment to Databricks:
pip install runw[databricks]
```

### 2. Create a project

```bash
runw init my_project
cd my_project
```

This scaffolds everything you need:

```
my_project/
  runw.toml              ← project & deploy config
  .env.example           ← Databricks credentials template
  pipelines/main.py      ← starter pipeline
  data/                  ← your data files
  test_quotes/           ← JSON payloads for pre-deploy testing
```

### 3. Write a pipeline

```python
# pipelines/main.py
import polars as pl
import runw

pipeline = runw.Pipeline("motor_pricing", description="Motor premium calculation")


@pipeline.node(path="data/policies.parquet", deploy_input=True)
def policies() -> pl.DataFrame:
    """Read policy data — this is the live API input."""
    return pl.scan_parquet("data/policies.parquet")


@pipeline.node(external="models/freq.cbm", file_type="catboost", model_class="regressor")
def frequency_model(policies: pl.DataFrame) -> pl.DataFrame:
    """Predict claim frequency."""
    df = policies.with_columns(
        freq_pred=pl.Series(obj.predict(policies.select("Area", "VehPower", "DrivAge").to_numpy()))
    )
    return df


@pipeline.node
def calculate_premium(frequency_model: pl.DataFrame) -> pl.DataFrame:
    """Calculate the technical premium."""
    return frequency_model.with_columns(
        premium=(pl.col("freq_pred") * 500).round(2)
    )


@pipeline.node(output=True)
def output(calculate_premium: pl.DataFrame) -> pl.DataFrame:
    """Final output returned by the API."""
    return calculate_premium
```

### 4. Run it

```bash
runw run
```

### 5. Open the GUI

```bash
runw serve
```

This opens a browser-based visual editor where you can:

- Drag and drop nodes from a palette
- Connect them with edges to define data flow
- Write Polars code in each transform node
- Click any node to preview its output data
- Toggle **API Input** on a data source to mark it as the live input
- Hit **Run** to execute the full pipeline
- Hit **Save** to write back to `.py`

### 6. Deploy

```bash
cp .env.example .env     # fill in your Databricks credentials
runw deploy
```

That's it. Your pipeline is now a live API on Databricks Model Serving.

---

## Deployment

Runway deploys your pipeline as an MLflow model on Databricks Model Serving. One command, no DevOps.

### How it works

1. **Marks** — you tag one data source as `deploy_input=True` (the live API input) and one node as `output=True` (the API response)
2. **Prunes** — Runway traces backwards from the output node and deploys only the scoring path. Training data branches, sinks, and exploratory nodes are automatically excluded
3. **Bundles** — model files (`.cbm`, `.pkl`, etc.) and static data are packaged as MLflow artifacts
4. **Validates** — every JSON file in `test_quotes/` is scored through the pruned pipeline before deployment. If anything fails, deployment is blocked
5. **Deploys** — the pipeline is logged as an MLflow pyfunc model and registered in the Model Registry

### Configuration

All deploy settings live in `runw.toml` (committed to git):

```toml
[project]
name = "motor-pricing"
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

Secrets go in `.env` (gitignored):

```bash
DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
DATABRICKS_TOKEN=your_token_here
```

### Test quotes

Put JSON files in `test_quotes/` with example requests. These are scored before every deploy:

```json
[
  {"IDpol": 99001, "VehPower": 7, "DrivAge": 42, "Area": "C", "VehBrand": "B12"}
]
```

### Dry run

Validate everything without actually deploying:

```bash
runw deploy --dry-run
```

```
  ✓ Loaded config from runw.toml
  ✓ Parsed pipeline (12 nodes, 14 edges)
  ✓ Pruned to output ancestors (5 nodes)
  ✓ Collected 2 artifacts (freq.cbm, sev.cbm)
  ✓ Inferred input schema (10 columns)
  ✓ Test quotes: single_policy.json     1 rows  ok  (18ms)
  ✓ Test quotes: batch_policies.json    5 rows  ok  (24ms)
  ✓ Validation passed
  Dry run complete — no model was deployed.
```

### Calling the deployed API

```bash
curl -X POST https://<workspace>.databricks.net/serving-endpoints/motor-pricing/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"dataframe_records": [{"Area": "A", "VehPower": 5, "DrivAge": 35}]}'
```

---

## Key Concepts

### Pipelines

A pipeline is a DAG of decorated Python functions. Each function is a **node** that takes DataFrames in and returns a DataFrame out.

```python
pipeline = runw.Pipeline("my_pipeline")

@pipeline.node
def transform(read_data: pl.DataFrame) -> pl.DataFrame:
    return read_data.filter(pl.col("age") > 25)
```

### Edges

Edges define data flow. Function parameter names match upstream node names:

```python
pipeline.connect("read_data", "transform")
pipeline.connect("transform", "output")
```

### Fan-out / Fan-in

One node can feed multiple downstream nodes, and a node can receive multiple inputs:

```python
@pipeline.node
def joined(claims: pl.DataFrame, exposure: pl.DataFrame) -> pl.DataFrame:
    return claims.join(exposure, on="IDpol", how="left")

pipeline.connect("claims", "joined")
pipeline.connect("exposure", "joined")
```

### Scoring

The same pipeline code works for batch and live scoring:

```python
# Batch: run the full pipeline
result = pipeline.run()

# Live: score a single row (same code path as the deployed API)
row = pl.DataFrame({"Area": ["A"], "DrivAge": [35]})
prediction = pipeline.score(row)
```

---

## Node Types

### Data Source

Reads data from a file. No code needed — just configure the path.

```python
@pipeline.node(path="data/policies.parquet", deploy_input=True)
def policies() -> pl.DataFrame:
    return pl.scan_parquet("data/policies.parquet")
```

- **`deploy_input=True`** — marks this source as the live API input for deployment
- Supported formats: **Parquet**, **CSV**, **JSON**

### Transform

The workhorse node. Write Polars code to filter, join, aggregate, or reshape data.

```python
@pipeline.node
def frequency_set(policies: pl.DataFrame, claims: pl.DataFrame) -> pl.DataFrame:
    return policies.join(claims, on="IDpol", how="left")
```

In the GUI, two shorthand syntaxes are available:

- **Chain syntax** — start with `.` to chain off the first input:
  `.filter(pl.col("Area") == "A").select("IDpol", "premium")`
- **Expression syntax** — reference multiple inputs by name:
  `policies.join(claims, on="IDpol", how="left")`

### External File

Load a model or config file, then use it in your code. The loaded object is available as `obj`.

```python
@pipeline.node(external="models/freq.cbm", file_type="catboost", model_class="regressor")
def frequency_model(policies: pl.DataFrame) -> pl.DataFrame:
    df = policies.with_columns(
        freq_pred=pl.Series(obj.predict(policies.select("Area", "VehAge").to_numpy()))
    )
    return df
```

| Type | Extension | How it loads |
|---|---|---|
| **Pickle** | `.pkl` | `pickle.load()` |
| **JSON** | `.json` | `json.load()` |
| **Joblib** | `.joblib` | `joblib.load()` |
| **CatBoost** | `.cbm` | `CatBoostClassifier` / `CatBoostRegressor` |

### Output

Marks the final node whose result becomes the API response:

```python
@pipeline.node(output=True)
def output(calculate_premium: pl.DataFrame) -> pl.DataFrame:
    return calculate_premium
```

### Data Sink

Writes data to disk. Sinks are pass-through during normal runs — writing only happens when you click **Write** in the GUI.

```python
@pipeline.node(sink="output/frequency.parquet", format="parquet")
def frequency_write(frequency_set: pl.DataFrame) -> pl.DataFrame:
    return frequency_set
```

---

## GUI

The visual editor runs in your browser at `http://localhost:5173`.

| Area | Description |
|---|---|
| **Left palette** | Drag node types onto the canvas |
| **Center canvas** | Visual DAG with drag, zoom, connect |
| **Right panel** | Configure the selected node |
| **Bottom panel** | Data preview — click any node to see its output |

Nodes marked `deploy_input=True` show a green **API** badge. Toggle it on/off in the node's config panel.

### Code ↔ GUI sync

Everything round-trips:

- Edit in the GUI → saves back to `.py`
- Edit the `.py` in your text editor → GUI picks it up on next load
- Custom imports, helper functions, and constants are preserved in both directions

---

## Pipeline Imports & Helpers

Every pipeline starts with `import polars as pl` and `import runw`. Add extra imports or helper functions via the **Imports** button (⚙) in the GUI toolbar, or write them directly in the `.py` file between the standard imports and the first `@pipeline.node`.

```python
import numpy as np
from catboost import CatBoostClassifier

DISCOUNT_RATE = 0.95

def apply_discount(df, col):
    return df.with_columns(pl.col(col) * DISCOUNT_RATE)
```

---

## CLI Reference

| Command | Description |
|---|---|
| `runw init <name>` | Scaffold a new project with config, starter pipeline, and test quotes |
| `runw run [file]` | Execute a pipeline and print results |
| `runw serve` | Start the visual editor |
| `runw deploy [file]` | Deploy the pipeline as a live API |
| `runw deploy --dry-run` | Validate and score test quotes without deploying |
| `runw status [model]` | Check the status of a deployed model |

### `runw serve` options

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Host to bind to |
| `--port` | `8000` | Backend API port |
| `--no-browser` | off | Don't auto-open the browser |

### `runw deploy` options

| Flag | Description |
|---|---|
| `--model-name` | Override model name from `runw.toml` |
| `--dry-run` | Validate and score test quotes without deploying |

---

## Project Structure

After `runw init`, your project looks like:

```
my_project/
  runw.toml                ← project & deploy config (committed)
  .env.example             ← Databricks credentials template (committed)
  .env                     ← actual credentials (gitignored)
  .gitignore
  pipelines/
    main.py                ← pipeline code (source of truth)
    main.runw.json         ← GUI layout state (node positions)
  data/                    ← data files (.parquet, .csv)
  test_quotes/             ← JSON payloads for pre-deploy validation
    example.json
```

- **`.py`** files are the source of truth — diffable, reviewable, testable
- **`.runw.json`** files store GUI layout (node positions) — not execution logic
- **`runw.toml`** is the single config file for project settings and deployment

---

## Design Principles

1. **Code is the source of truth** — the `.py` file is the pipeline. The GUI is a view.
2. **Same pipeline, every context** — the same code runs for 1-row live quotes and million-row batch jobs.
3. **Real Python, real Polars** — no proprietary formula language. Your skills transfer.
4. **Git-native** — pipelines are plain files. Diff, review, branch, merge.
5. **One-command deploy** — `runw deploy` handles pruning, bundling, validation, and MLflow registration.
6. **Testable** — every node is a plain function. `pytest` just works.

---

## Requirements

- Python >= 3.11
- For deployment: `pip install runw[databricks]` (adds MLflow + Databricks SDK)
- Works on **Linux**, **macOS**, and **Windows**

---

## License

MIT