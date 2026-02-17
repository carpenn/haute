---
hide:
  - navigation
  - toc
---

## What is Haute?

Haute is a free, open-source pricing engine for insurance teams. It lets you build rating pipelines in a visual editor, keep everything as standard Python, and deploy to a live scoring API with a single command.

It's designed for pricing teams who want to move faster without giving up control - of their code, their infrastructure, or their deployment process.

---

## The problem it solves

Getting a pricing model from someone's laptop into a live system that can serve real quotes is hard. The typical path involves weeks of handoff between actuaries and engineering, deployment scripts that only one person understands, and no easy way to see what a change will actually do to the book before it goes live.

Haute handles that entire path. You build a pipeline, Haute packages it, tests it, deploys it to a staging environment, shows you the impact, and promotes it to production when you're ready. The whole process runs automatically every time you make a change.

---

## How it works

A pricing pipeline in Haute is a Python file. Standard Python, using [Polars](https://pola.rs/) DataFrames. Each step in your pipeline is a function - load data, apply a model, calculate a premium.

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

That same file is also a visual graph. Haute runs a browser-based editor where your pipeline appears as a drag-and-drop flow diagram - nodes for data sources, models, transforms, and outputs, connected by edges that show how data moves through the rating structure.

**The key bit: these are the same thing.** Edit the code and the graph updates. Edit the graph and the code updates. There's no import/export step, no "generate code" button. They're always in sync.

This means the people who prefer working visually can do that, and the people who prefer working in code can do that, and nobody has to choose.

---

## What makes it different

### Your code, not ours

Pipelines are plain `.py` files. Not a proprietary format, not a visual-only diagram, not a config language that looks like code but isn't. You can read it, test it, version-control it, and take it with you if you ever stop using Haute. Any model that runs in Python works here - scikit-learn, CatBoost, LightGBM, XGBoost, whatever comes next.

### Safety is not optional

One wrong factor can misprice an entire book before anyone notices. So Haute doesn't let you skip the safety steps.

Every deployment follows the same path: deploy to a private staging copy, score a portfolio sample through both the new and current models, produce an impact report showing what changed and by how much, and wait for a human to approve before going live. There are no shortcuts because there shouldn't be.

Every release records what changed, who approved it, the impact report, and what version it replaced - covering the traceability requirements that regulators expect.

### Deploy anywhere

You pick the target that matches your infrastructure. Haute handles the rest - and analysts never need Docker, cloud CLIs, or DevOps tooling installed. CI builds and deploys everything.

| Target | What gets deployed |
|---|---|
| **Databricks** | MLflow model on Databricks Model Serving |
| **Container** | Docker image with a FastAPI server - runs anywhere |
| **Azure Container Apps** | Same image, deployed as an ACA revision |
| **AWS ECS** | Same image, deployed as an ECS task |
| **GCP Cloud Run** | Same image, deployed as a Cloud Run service |

All container-based targets produce the same thing: a FastAPI app with `POST /quote` and `GET /health`, packaged into a Docker image. The generic container target is for teams who manage their own infrastructure. The platform targets let Haute handle the full lifecycle.

### One pipeline, many uses

The same pipeline file works for live quoting (a single request, sub-second response), batch scoring (millions of rows), what-if analysis (drag sliders to see how price changes through each step), and execution tracing (click any output price and see the path through every node that produced it).

### Version control and CI/CD come free

Because pipelines are files in Git, you get branching, pull requests, code review, full history, and rollback for free. When you set up a project, Haute generates the CI/CD configuration for your team's platform - GitHub Actions, GitLab CI, or Azure DevOps. Every proposed change gets tested and validated automatically before anyone reviews it.

---

## Quick start

```
uv add haute
haute init --target databricks --ci github
haute serve
```

Three commands to go from nothing to a working project open in the visual editor. See the **[Getting Started](getting-started/index.md)** guide for the full walkthrough.
