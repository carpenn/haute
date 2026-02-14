# Runway

**Open-source pricing engine for insurance teams on Databricks.**

Build, visualise, and deploy pricing pipelines as Python code — with a browser-based GUI that stays in sync.

```
pip install runw
```

---

## What is Runway?

Runway gives insurance pricing teams a **code-first, GUI-friendly** way to build data pipelines. It bridges the gap between:

- **Pricing analysts** who are comfortable with visual tools (Alteryx, WTW Radar)
- **Engineering best practices**: version control, CI/CD, unit tests, code review

The core principle: **Python code is the source of truth**. The GUI is a live, editable view of that code.

### Runway is:

- A Python DSL for defining pricing pipelines as decorated functions
- A browser-based React Flow UI for visual pipeline editing
- A Polars-powered execution engine (fast, lazy, streaming)
- A CLI for scaffolding, running, and serving pipelines

### Runway is not:

- A model training framework (use MLflow, scikit-learn, XGBoost, etc.)
- A full ETL platform (use Spark, dbt, etc. for heavy lifting)
- A replacement for Databricks — Runway is designed to run **on** Databricks

---

## Quick Start

### 1. Install

```bash
pip install runw
```

Or with uv:

```bash
uv add runw
```

### 2. Create a project

```bash
runw init my_project
cd my_project
```

### 3. Write a pipeline

```python
"""pipelines/main.py"""

import polars as pl
import runw

pipeline = runw.Pipeline("area_pricing", description="Assign premium by area")


@pipeline.node(path="data/policies.parquet")
def read_data() -> pl.DataFrame:
    """Read policy data."""
    return pl.read_parquet("data/policies.parquet")


@pipeline.node
def area_premium(read_data: pl.DataFrame) -> pl.DataFrame:
    """Assign premium based on Area column."""
    return read_data.with_columns(
        pl.when(pl.col("Area") == "A").then(100)
          .when(pl.col("Area") == "B").then(200)
          .otherwise(300)
          .alias("premium")
    )


@pipeline.node
def output(area_premium: pl.DataFrame) -> pl.DataFrame:
    """Final output."""
    return area_premium.select("IDpol", "Area", "premium")
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
- Hit **Run** to execute the full pipeline
- Hit **Save** to persist as `.py` + `.json`

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

Edges define data flow. The output of one node becomes the input of the next. Function parameter names match the upstream node names.

```python
pipeline.connect("read_data", "transform")
pipeline.connect("transform", "output")
```

### Fan-out / Fan-in

One node can feed multiple downstream nodes (fan-out), and a node can receive multiple inputs (fan-in):

```python
@pipeline.node
def joined(Region: pl.DataFrame, AvgAge: pl.DataFrame) -> pl.DataFrame:
    """Join two upstream DataFrames."""
    return Region.join(AvgAge, on="DrivAge", how="left")

pipeline.connect("read_data", "Region")
pipeline.connect("read_data", "AvgAge")
pipeline.connect("Region", "joined")
pipeline.connect("AvgAge", "joined")
```

### Scoring

The same pipeline code works for batch and live scoring:

```python
# Batch: run the full pipeline
result = pipeline.run()

# Live: score a single row
row = pl.DataFrame({"Area": ["A"], "DrivAge": [35]})
prediction = pipeline.score(row)
```

---

## GUI

The visual editor is built with React Flow and runs in your browser.

### Layout

| Area | Description |
|---|---|
| **Left palette** | Drag node types onto the canvas |
| **Center canvas** | Visual DAG with drag, zoom, connect |
| **Right panel** | Configure the selected node |
| **Bottom panel** | Data preview — click any node to see its output |

### Code Editor

Transform nodes include an inline Polars code editor. Two syntaxes:

**Chain syntax** — start with `.` to chain off the input:
```
.filter(pl.col("Area") == "A")
.select("IDpol", "Area", "premium")
```

**Expression syntax** — reference inputs by their node name:
```
Region.join(AvgAge, on="DrivAge", how="left")
```

The system auto-wraps your code into a proper function. No boilerplate needed.

---

## Node Types

Runway provides several node types, each designed for a specific role in your pipeline.

### Data Source

Reads data from a file or table. No code needed — just configure the path.

```python
@pipeline.node(path="data/policies.parquet")
def policies() -> pl.DataFrame:
    """Read policy data."""
    return pl.scan_parquet("data/policies.parquet")
```

Supported formats: **Parquet**, **CSV**, **JSON**. Databricks tables are planned.

### Transform (Polars)

The workhorse node. Write Polars code to filter, join, aggregate, or reshape data. Inputs are referenced by their upstream node name.

```python
@pipeline.node
def frequency_set(policies: pl.DataFrame, claims: pl.DataFrame) -> pl.DataFrame:
    """Join policies with claims."""
    df = (
    policies
    .join(claims, on="IDpol", how="left")
    .with_columns(
        ClaimCount=pl.when(pl.col("ClaimAmount") > 0).then(1).otherwise(0)
    )
    )
    return df
```

In the GUI code editor, you can use two shorthand syntaxes:

- **Chain syntax** — start with `.` to chain off the first input:
  ```
  .filter(pl.col("Area") == "A")
  .select("IDpol", "premium")
  ```
- **Expression syntax** — reference multiple inputs by name:
  ```
  policies.join(claims, on="IDpol", how="left")
  ```

The system wraps your code into a proper function automatically.

### External File

Load an external object (model, config, lookup) from a file in your project, then write code that uses it alongside your DataFrames. The loaded object is available as `obj` in the code editor.

```python
@pipeline.node(external="models/model.cbm", file_type="catboost", model_class="regressor")
def score(policies: pl.DataFrame) -> pl.DataFrame:
    """Score policies with CatBoost model."""
    from catboost import CatBoostRegressor
    obj = CatBoostRegressor()
    obj.load_model("models/model.cbm")
    df = (
    policies.with_columns(
        prediction=pl.Series(obj.predict(policies.select("Area", "VehAge").to_numpy()))
    )
    )
    return df
```

Supported file types:

| Type | Extension | Loading |
|---|---|---|
| **Pickle** | `.pkl`, `.pickle` | `pickle.load()` |
| **JSON** | `.json` | `json.load()` |
| **Joblib** | `.joblib` | `joblib.load()` |
| **CatBoost** | `.cbm` | `CatBoostClassifier()` or `CatBoostRegressor()` with `.load_model()` |

For CatBoost models, you also choose **Classifier** or **Regressor** in the GUI — the correct class is used automatically.

### Data Sink

Write a DataFrame to disk. Sinks are **pass-through** during normal runs and previews — the data flows through without writing. Writing only happens when you explicitly click the **Write** button in the node's config panel.

```python
@pipeline.node(sink="output/frequency.parquet", format="parquet")
def frequency_write(frequency_set: pl.DataFrame) -> pl.DataFrame:
    """Write frequency data to parquet."""
    frequency_set.collect().write_parquet("output/frequency.parquet")
    return frequency_set
```

Supported formats: **Parquet**, **CSV**.

---

## Pipeline Imports & Helpers

By default, every pipeline includes `import polars as pl` and `import runw`. If your nodes need additional libraries, helper functions, or constants, add them via the **Imports** button in the toolbar (the ⚙ icon).

Everything you put here is written between the standard imports and your node functions in the `.py` file, and is **preserved across GUI saves**.

### Example

In the GUI, click **Imports** and add:

```python
import numpy as np
from catboost import CatBoostClassifier

DISCOUNT_RATE = 0.95

def apply_discount(df, col):
    return df.with_columns(pl.col(col) * DISCOUNT_RATE)
```

This produces a `.py` file like:

```python
"""Pipeline: my_pipeline"""

import polars as pl
import runw

import numpy as np
from catboost import CatBoostClassifier

DISCOUNT_RATE = 0.95

def apply_discount(df, col):
    return df.with_columns(pl.col(col) * DISCOUNT_RATE)

pipeline = runw.Pipeline("my_pipeline", description="")


@pipeline.node(path="data/policies.parquet")
def policies() -> pl.DataFrame:
    ...
```

### Round-tripping

The preamble works in both directions:

- **GUI → Code**: when you save from the GUI, your imports and helpers are written to the `.py` file
- **Code → GUI**: when you edit the `.py` file directly and add imports, they appear in the Imports panel next time the GUI loads

This means you can freely switch between the GUI and your text editor without losing custom code.

---

## CLI Reference

| Command | Description |
|---|---|
| `runw init <name>` | Scaffold a new project |
| `runw run [file]` | Execute a pipeline and print the result |
| `runw serve` | Start the GUI (FastAPI + Vite dev server) |

### `runw serve` options

```
--host      Host to bind to (default: 127.0.0.1)
--port      Backend API port (default: 8000)
--no-browser  Don't auto-open the browser
```

---

## Project Structure

```
my_project/
├── pipelines/          # Your pipeline .py and .json files
│   ├── main.py         # Pipeline code (source of truth)
│   └── main.json       # GUI state (positions, config)
├── data/               # Data files (.parquet, .csv)
├── examples/           # Example pipelines
└── pyproject.toml
```

Pipelines are saved as:
- **`.py`** — executable Python code, Git-friendly, reviewable
- **`.json`** — GUI layout state (node positions, edge routing)

---

## Tech Stack

### Backend

| Concern | Library |
|---|---|
| Language | Python >= 3.11 |
| Package manager | uv |
| Web framework | FastAPI |
| Data engine | Polars |
| CLI | Click |
| Build | Hatchling |

### Frontend

| Concern | Library |
|---|---|
| Framework | React 19 |
| Flow editor | React Flow |
| Language | TypeScript |
| Styling | Tailwind CSS v4 |
| Bundler | Vite |
| Icons | Lucide |

### Optional

| Concern | Library |
|---|---|
| Model registry | MLflow |
| Data platform | Databricks SDK |

---

## Development

```bash
# Clone
git clone https://github.com/PricingFrontier/runway.git
cd runway

# Install Python deps
uv sync

# Install frontend deps
cd frontend && npm install && cd ..

# Run in dev mode (hot reload on both sides)
runw serve
```

The dev server runs:
- **Vite** on `http://localhost:5173` (frontend with HMR)
- **FastAPI** on `http://localhost:8000` (API, auto-reloads on Python changes)

Open `http://localhost:5173` — Vite proxies API calls to the backend automatically.

### Running tests

```bash
uv run pytest
```

### Linting

```bash
uv run ruff check src/
```

---

## Design Philosophy

1. **Code is the source of truth** — the `.py` file is the pipeline. The GUI is a view.
2. **Real Python, real Polars** — no proprietary formula language. Skills transfer.
3. **Git-native** — pipelines are plain Python files. Diff, review, branch, merge.
4. **Fast by default** — Polars is Rust-based. 678K rows preview instantly.
5. **Testable** — every node is a plain function. `pytest` just works.
6. **Deployable** — `pipeline.score(df)` works in any API, notebook, or Databricks job.

---

## License

MIT