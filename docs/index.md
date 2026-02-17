---
hide:
  - navigation
---

<div align="center" markdown>

# Haute

### Open-source pricing engine for insurance.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Elastic License 2.0](https://img.shields.io/badge/license-Elastic_2.0-blue?style=flat-square)](LICENSE)
[![Databricks](https://img.shields.io/badge/deploy-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Docker](https://img.shields.io/badge/deploy-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

Haute is a free pricing engine that lets insurance teams **build rating pipelines in a visual editor**, keep everything as **version-controlled Python**, and **deploy to a live scoring API** — on Databricks, AWS, Azure, or a plain Docker container.

It bridges the gap between enterprise platforms like WTW Radar (six- or seven-figure annual licences, proprietary formats, vendor lock-in) and in-house Python notebooks that never quite make it to production.

---

## Why Haute?

<div class="grid cards" markdown>

-   :material-code-braces:{ .lg .middle } **Code-native**

    ---

    Pipelines are standard Python + Polars. No proprietary DSL, no black boxes. Works with any model framework.

-   :material-graph:{ .lg .middle } **Visual editor**

    ---

    Browser-based drag-and-drop flow editor powered by React Flow. Edit visually or in code — they stay in sync.

-   :material-rocket-launch:{ .lg .middle } **One-command deploy**

    ---

    `haute deploy` packages your pipeline and pushes it to Databricks, Docker, AWS, or Azure as a live scoring API.

-   :material-shield-check:{ .lg .middle } **Safety built in**

    ---

    Staging environments, impact analysis, smoke tests, and approval gates — baked into every deployment.

-   :material-git:{ .lg .middle } **Git-native**

    ---

    Branching, pull requests, full history, rollback. Every change is reviewed before it goes live.

-   :material-currency-usd-off:{ .lg .middle } **Free**

    ---

    Elastic License 2.0. No per-user fees, no seat limits. The only restriction is you can't resell Haute as a hosted service.

</div>

---

## How it works

A pricing pipeline in Haute is a Python file — standard Python, using Polars DataFrames. It lives in Git and is testable with pytest.

But it's also a visual graph. Haute runs a browser-based editor where the pipeline is rendered as a drag-and-drop flow diagram. You can click into any node and see its output data.

**These are the same thing.** The visual editor reads from and writes to the Python file. Change a node in the GUI and the `.py` file updates on disk. Edit the `.py` file and the graph updates in the browser. No import/export step, no translation layer.

```python
import haute
import polars as pl

pipeline = haute.Pipeline("motor_pricing")

@pipeline.node
def policies(path="data/policies.parquet") -> pl.DataFrame:
    return pl.read_parquet(path)

@pipeline.node
def frequency_model(policies: pl.DataFrame) -> pl.DataFrame:
    model = haute.load_model("models/freq.cbm")
    return policies.with_columns(pred_freq=model.predict(policies))

@pipeline.node
def calculate_premium(frequency_model: pl.DataFrame) -> pl.DataFrame:
    return frequency_model.with_columns(
        premium=pl.col("pred_freq") * pl.col("pred_sev") * 1.15
    )
```

---

## Deploy targets

| Target | What gets deployed |
|---|---|
| **Databricks** | MLflow model on Databricks Model Serving |
| **Docker** | Self-contained container with a FastAPI server — runs anywhere |
| **AWS SageMaker** | MLflow model on a SageMaker real-time endpoint |
| **Azure ML** | MLflow model on an Azure ML managed endpoint |

---

## Quick start

```bash
pip install haute
haute init --target databricks --ci github
# edit your pipeline...
haute serve    # open the visual editor
haute deploy   # push to production
```

