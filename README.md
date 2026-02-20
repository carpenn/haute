<div align="center">

# ✦ Haute

### Open-source pricing engine for insurance.

<br>

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Elastic License 2.0](https://img.shields.io/badge/license-Elastic_2.0-blue?style=flat-square)](LICENSE)
[![Databricks](https://img.shields.io/badge/deploy-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Azure Container Apps](https://img.shields.io/badge/deploy-Azure_Container_Apps-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/container-apps)
[![AWS ECS](https://img.shields.io/badge/deploy-AWS_ECS-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/ecs/)
[![GCP Cloud Run](https://img.shields.io/badge/deploy-Cloud_Run-4285F4?style=flat-square&logo=googlecloud&logoColor=white)](https://cloud.google.com/run)
[![Docker](https://img.shields.io/badge/deploy-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

Haute is a free, open-source pricing engine that lets insurance teams build rating pipelines in a visual editor, keep everything as version-controlled Python, and deploy to a live scoring API with built-in safety and audit trails.

---

## Quick start

```bash
pip install haute
haute init my-project --target databricks --ci github
cd my-project
haute serve
```

`haute serve` opens a browser-based visual editor. Drag, connect, and configure nodes - or edit the Python file directly. They stay in sync via WebSocket.

---

## Code ↔ GUI sync

A pricing pipeline in Haute is a Python file. Standard Python, using Polars DataFrames. It lives in Git, you can open it in any editor, and it's testable with pytest like any other code.

But it's also a visual graph. Haute runs a browser-based editor where the pipeline is rendered as a drag-and-drop flow diagram - nodes for data sources, transforms, models, and outputs, connected by edges that show how data moves through the rating structure. You can click into any node and see its output data.

The key bit: **these are the same thing**. The visual editor reads from and writes to the Python file. Change a node in the GUI and the `.py` file updates on disk. Edit the `.py` file and the graph updates in the browser. There's no import/export step, no translation layer, no "generate code" button. They're always in sync.

This means analysts can work visually while everything stays in version control. Technical people can work in code without losing the visual representation. And nobody has to choose one or the other.

---

## Polars-native execution engine

Pipelines run on [Polars](https://pola.rs/) LazyFrames end-to-end. Haute builds a lazy query plan across your entire graph - source nodes, transforms, model scores, joins, outputs - and lets Polars optimise the full plan before collecting results. This means predicate pushdown, projection pushdown, and parallel execution happen automatically.

During preview and tracing, Haute switches to eager single-pass execution with a per-graph cache. The pipeline is fingerprinted by its structure and code content - if nothing changed, clicking a different node reuses the cached DataFrames instantly instead of re-executing model scoring on hundreds of thousands of rows.

Any model that runs in Python works here: scikit-learn GLMs, CatBoost, LightGBM, XGBoost, or anything else. Nodes are plain functions that take and return DataFrames, so standard testing tools just work.

---

## Databricks connector

Data source nodes can point directly at Databricks Unity Catalog tables. Haute streams data from Databricks SQL via Arrow batches (100K rows at a time, zstd-compressed), writes it to a local `.parquet` cache, and from there every pipeline run reads from that cache with full `scan_parquet` speed - predicate pushdown, zero-copy reads, no round-trips.

The fetch is incremental and memory-bounded: a `ParquetWriter` writes each Arrow batch to disk as it arrives, so even multi-million-row tables don't spike memory. Writes go to a temporary file first and atomically rename on success, so a failed fetch never leaves a corrupt cache behind.

From the GUI, you browse catalogs, schemas, and tables directly - pick a table, click Fetch, and the data is ready. The connector also supports custom SQL queries if you only need a subset.

---

## Execution tracing

Click any output price and trace the full path through every node that produced it, with intermediate values at each step:

```
base rate £300  →  area factor ×1.2  →  NCD ×0.85  →  frequency load ×1.35  →  £412.50
```

The tracer runs the pipeline on a single row and captures per-node snapshots: input schema, output schema, row values, and a column-level diff (added, removed, modified, passed through). When you trace a specific column, Haute prunes the graph to only the nodes that contributed to that value - irrelevant branches disappear.

Traces are cached by graph fingerprint. The first click executes the full pipeline (~1-2s on large datasets); subsequent clicks with a different row or column reuse the cached DataFrames and extract a different slice in under 10ms.

This is useful for regulatory explainability (Solvency II, IFRS 17, FCA pricing practices), but it's also just a good way to debug a pipeline and understand what's happening at each step.

---

## Submodels

Group nodes into reusable submodels - separate `.py` files that the main pipeline imports. Select nodes in the GUI, click Group, and Haute extracts them into `modules/<name>.py` with their own internal edges. The main pipeline gets a single submodel node with auto-wired input/output ports.

Submodels can be drilled into (double-click to see the internal graph), dissolved back into the parent pipeline, and reused across multiple pipelines. They're regular Python files with a `haute.Submodel` class, so they work with imports, tests, and linting like anything else.

---

## Deployment

Haute deploys pipelines as live scoring APIs. You set the target once in a config file, and from there every release is handled by CI - build, push, smoke test, impact analysis, production promotion. Analysts never need Docker, cloud CLIs, or DevOps tooling installed locally.

| Target | What gets deployed | Status |
|---|---|---|
| **Databricks** | MLflow model on Databricks Model Serving | Implemented |
| **Container** | Docker image with a FastAPI server - runs anywhere | Implemented |
| **Azure Container Apps** | Same image, deployed as an ACA revision | Build+push implemented |
| **AWS ECS** | Same image, deployed as an ECS task | Build+push implemented |
| **GCP Cloud Run** | Same image, deployed as a Cloud Run service | Build+push implemented |

All container-based targets share the same build: a FastAPI app wrapping `score_graph()` with `POST /quote` and `GET /health`, packaged into a Docker image, pushed to a registry. The generic container target is there for teams who manage their own infrastructure - Kubernetes, on-prem, or local testing. The platform targets handle the full lifecycle.

### What runs where

Analysts only need Python installed. Everything else happens in CI.

| Command | Where it runs | What it needs |
|---|---|---|
| `haute init` | Local | Python |
| `haute serve` | Local | Python |
| `haute deploy` | CI only | Docker + cloud creds (CI runner has both) |
| `haute smoke` | CI only | Cloud creds |
| `haute impact` | CI only | Cloud creds |

---

## Safety

A pricing engine without safety rails is dangerous - one wrong factor can misprice an entire book before anyone notices. Deployment safety isn't optional in Haute, it's baked into how the tool works.

Before any model reaches production, Haute scores a set of example quotes through the pipeline. If anything breaks - a missing column, a model that won't load, a transform that errors on edge cases - the deployment is blocked. You can also set expected outputs with tolerances, so you're testing not just "does it run" but "does it produce the right prices".

When a new model is ready, it gets deployed to a staging environment first. Haute then scores a portfolio sample through both the new model and the one currently in production, and produces a comparison report: how many quotes changed, by how much, which segments moved, what the overall premium impact looks like. The people who need to approve the change see the financial effect before it goes live.

The path to production is always the same: deploy to staging, verify it works, review the impact, then manually promote. Every deployment records what changed, who approved it, the impact report, and what version it replaced.

---

## One pipeline, multiple uses

The same pipeline file works for:

- **Live quoting** - a single JSON request, sub-second response
- **Batch scoring** - millions of rows via the Polars lazy engine
- **What-if analysis** - drag sliders to see how price changes through each node
- **Impact analysis** - compare new vs current model across a portfolio sample
- **Execution tracing** - single-row lineage with per-node intermediate values

---

## Version control and CI/CD

Pipelines are plain `.py` files in Git. Branching, pull requests, full history, and rollback all work out of the box.

`haute init` generates CI configuration for your provider - GitHub Actions, GitLab CI, or Azure DevOps. The generated pipeline runs: validate → deploy to staging → smoke test → impact analysis → manual promotion to production. You don't have to build any of this yourself.

Deployments can only happen through this process. Nobody can push a model to production from their laptop. The only path to live is through the review pipeline.

---

## Architecture

| Layer | Technology |
|---|---|
| **Visual Editor** | React, TypeScript, React Flow |
| **Backend** | Python, FastAPI, Polars, WebSocket sync |
| **Models** | MLflow (registry, tracking, serving) |
| **Deploy Targets** | Databricks · Container · Azure Container Apps · AWS ECS · GCP Cloud Run |
| **CI/CD** | GitHub Actions · GitLab CI · Azure DevOps |

---

## License

Elastic License 2.0. Free to use for your team, your models, your infrastructure. The only restriction is that you can't sell Haute itself as a hosted service. See [LICENSE](LICENSE) for details.