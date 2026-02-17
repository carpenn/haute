---
template: home.html
hide:
  - navigation
  - toc
  - feedback
---

<style>
  /* Hide the default page title on the home page */
  .md-typeset h1 { display: none; }
</style>

## :material-help-circle-outline: What is Haute?

Haute is a free, open-source pricing engine for insurance teams. Build rating pipelines in a visual editor, keep everything as standard Python, and deploy to a live scoring API - all without leaving your IDE or waiting on engineering.

!!! tip "Who is this for?"
    Pricing teams who want to move faster without giving up control - of their code, their infrastructure, or their deployment process. If you're tired of six-figure platform licences or deployment scripts that only one person understands, this is for you.

---

## :material-target: The problem it solves

Getting a pricing model from someone's laptop into a live system that can serve real quotes is **hard**. The typical path:

<div class="grid cards" markdown>

- :material-swap-horizontal:{ .lg .middle } **Weeks of handoff**

    ---

    Actuaries build models, then wait for engineering to deploy them. Back and forth, every release.

- :material-lock:{ .lg .middle } **Deployment scripts nobody understands**

    ---

    One person wrote the deploy script. They left. Now nobody touches it.

- :material-eye-off:{ .lg .middle } **No visibility before go-live**

    ---

    "What will this do to the book?" Nobody knows until it's in production.

</div>

Haute handles that entire path. You build a pipeline, Haute packages it, tests it, deploys it to a staging environment, shows you the financial impact, and promotes it to production when you're ready. The whole process runs automatically every time you merge a change.

---

## :material-cog: How it works

A pricing pipeline in Haute is a Python file. Standard Python, using [Polars](https://pola.rs/) DataFrames. Each step is a function - load data, apply a model, calculate a premium.

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

That same file is also a visual graph. Haute runs a browser-based editor where your pipeline appears as a drag-and-drop flow diagram - nodes for data sources, models, transforms, and outputs, connected by edges showing how data flows through the rating structure.

!!! success "The key bit: these are the same thing"
    Edit the code and the graph updates. Edit the graph and the code updates. There's no import/export step, no "generate code" button. They're always in sync.

    Analysts work visually. Engineers work in code. Nobody has to choose.

---

## :material-star-shooting: What makes it different

<div class="grid cards" markdown>

- :material-language-python:{ .lg .middle } **Your code, not ours**

    ---

    Pipelines are plain `.py` files. Not a proprietary format, not a visual-only diagram. You can read it, test it with pytest, version-control it, and take it with you. Any model that runs in Python works here.

    [:octicons-arrow-right-24: Getting started](getting-started/index.md)

- :material-shield-check:{ .lg .middle } **Safety is not optional**

    ---

    Every release follows the same path: staging, impact analysis, smoke test, human approval, production. One wrong factor can misprice an entire book - so there are no shortcuts.

    [:octicons-arrow-right-24: Safety pipeline](deployment/index.md)

- :material-cloud-upload:{ .lg .middle } **Deploy anywhere**

    ---

    Databricks, Azure Container Apps, AWS ECS, GCP Cloud Run, or plain Docker. Analysts never need Docker or cloud CLIs installed - CI handles everything.

    [:octicons-arrow-right-24: Deploy targets](deployment/index.md)

- :material-source-branch:{ .lg .middle } **CI/CD comes free**

    ---

    `haute init` generates GitHub Actions, GitLab CI, or Azure DevOps pipelines. Every change is tested, validated, and impact-analysed before it reaches production.

    [:octicons-arrow-right-24: CI/CD setup](deployment/ci/github-actions.md)

</div>

---

## :material-rocket-launch: Deploy targets

You pick the target that matches your infrastructure. Haute handles the rest.

=== ":material-cloud: Container platforms"

    All container-based targets produce the same thing: a FastAPI app with `POST /quote` and `GET /health`, packaged into a Docker image. CI builds and pushes it - analysts never touch Docker.

    | Target | What happens |
    |---|---|
    | **Container** | Build + push to registry. You manage the service. |
    | **Azure Container Apps** | Build + push + create a new ACA revision |
    | **AWS ECS** | Build + push + update the ECS task |
    | **GCP Cloud Run** | Build + push + deploy a new Cloud Run revision |

=== ":material-database: Databricks"

    Wraps the pipeline in an MLflow `PythonModel` and deploys to Databricks Model Serving. For teams already on the Databricks platform.

---

## :material-account-hard-hat: What runs where

Analysts only need Python. Everything else happens in CI.

| | Command | Where | What it needs |
|---|---|---|---|
| :material-laptop: | `haute init` | Local | Python |
| :material-laptop: | `haute serve` | Local | Python |
| :material-robot: | `haute deploy` | CI only | Docker + cloud creds |
| :material-robot: | `haute smoke` | CI only | Cloud creds |
| :material-robot: | `haute impact` | CI only | Cloud creds |

!!! info "The analyst's workflow"
    Edit your pipeline :material-arrow-right: `haute serve` to preview :material-arrow-right: push to Git :material-arrow-right: CI does the rest.

    Deployments can only happen through this process. Nobody can push a model to production from their laptop.

---

## :material-speedometer: Quick start

```bash
uv add haute
haute init --target databricks --ci github
haute serve   # open the visual editor
```

Three commands to go from nothing to a working project open in the visual editor.

[:material-arrow-right: **Full getting started guide**](getting-started/index.md){ .md-button .md-button--primary }
[:material-github: **View on GitHub**](https://github.com/pricingfrontier/haute){ .md-button }
