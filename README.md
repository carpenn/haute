<div align="center">

# Haute

### Open-source pricing engine for insurance.

<br>

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![AGPL-3.0](https://img.shields.io/badge/license-AGPL--3.0-blue?style=flat-square)](LICENSE)
[![Databricks](https://img.shields.io/badge/deploy-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![Azure Container Apps](https://img.shields.io/badge/deploy-Azure_Container_Apps-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/container-apps)
[![AWS ECS](https://img.shields.io/badge/deploy-AWS_ECS-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/ecs/)
[![GCP Cloud Run](https://img.shields.io/badge/deploy-Cloud_Run-4285F4?style=flat-square&logo=googlecloud&logoColor=white)](https://cloud.google.com/run)
[![Docker](https://img.shields.io/badge/deploy-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

Insurance pricing has been stuck for years. The tools are expensive, proprietary, and slow. Models are locked inside platforms you don't control. Deploying a rate change takes weeks. Explaining a price to your regulator means assembling a manual audit trail after the fact. And if you want to leave, your work doesn't come with you.

Haute is a free, open-source pricing engine that changes how this works. You get a visual editor for building rating pipelines, a data engine fast enough to score 600,000 rows in seconds, price tracing that shows exactly how any output was calculated, and deployment that takes your pipeline live without needing to learn cloud infrastructure. Everything is a Python file on disk - no proprietary format, no lock-in, no annual licence negotiation.

---

## A visual editor, not a code editor

Haute opens in your browser. You build rating pipelines by dragging nodes onto a canvas and connecting them - data sources, transforms, model scores, rating steps, outputs. Click any node and its data appears instantly in a preview table below.

There are ten node types, each purpose-built for pricing work:

| Node | What it does |
|---|---|
| **Data Source** | Reads a file or a Databricks table |
| **API Input** | Receives live quote requests when deployed |
| **Polars Transform** | Cleans, joins, or reshapes data |
| **Model Score** | Scores records through a GLM, CatBoost, XGBoost, or any Python model |
| **Banding** | Groups continuous or categorical values into bands |
| **Rating Step** | Applies rating factors from a lookup table - multiply, add, cap, floor |
| **Live Switch** | Routes between live API data and batch data |
| **Output** | Defines the final price fields returned by your API |
| **Data Sink** | Writes results to a file |
| **Submodel** | Collapses a group of nodes into a single reusable block |

If you've built rating structures in spreadsheets or used visual pricing tools, this will feel familiar - except it runs on your machine, handles millions of rows, and you own everything.

---

## Click any price. See exactly how it was calculated.

This is the feature that changes everything for regulatory work.

Click any cell in your output table - say, a technical price of £412.50. Haute instantly traces the path through every node that contributed to it, showing the value at each step:

```
base rate £300  →  area factor ×1.2  →  NCD ×0.85  →  frequency load ×1.35  →  £412.50
```

The graph lights up. Nodes on the trace path glow. Nodes that didn't contribute fade away. A sidebar shows you what happened at every step - which columns were added, which were modified, what the values were before and after.

This isn't a static report you generate after the fact. It's live, interactive, and instant. Click a different row - the trace updates immediately from cache. Click a different column - same thing. First click takes about a second; every click after that is under 10 milliseconds.

For Solvency II, IFRS 17, and FCA pricing practices, you need to show how a price was derived. Haute makes that a click, not a project.

---

## Fast enough that you stop waiting

Pricing analysts spend a shocking amount of time waiting. Waiting for a portfolio to score. Waiting for a notebook to run. Waiting for results to refresh after changing one factor.

Haute uses Polars - a data engine built for speed. A 600,000-row portfolio scores in seconds, not minutes. Joining large rating tables, applying banding logic, running model predictions - all of it processes in parallel automatically.

The editor is fast too. When you click between nodes, Haute doesn't re-run everything from scratch. It caches results intelligently, so previewing data at any point in your pipeline is near-instant. Change a rating factor and only the downstream nodes recalculate.

There's a timing breakdown built into the preview panel - a bar chart showing how long each node took, colour-coded green/yellow/red. You always know what's slow and why.

---

## Connect to your data where it lives

Point a Data Source node at a Databricks Unity Catalog table. Browse your catalogs, schemas, and tables directly from the editor - no SQL required. Pick your SQL warehouse, select a table, click Fetch.

The data streams down in batches. Even tables with millions of rows download efficiently without exhausting your machine's memory. If a download fails halfway through, nothing is corrupted - the cache only updates when the full download succeeds.

Once fetched, the data is cached locally. Your pipeline runs at full speed on your machine without needing a live connection to Databricks. You can see row counts, column counts, file sizes, and clear the cache whenever you want.

Need a subset? Write a SQL query directly in the node configuration.

---

## One wrong factor can misprice an entire book

Haute is opinionated about preventing this.

**Before anything goes live**, Haute scores a set of test quotes through your pipeline. If anything breaks - a missing column, a model that won't load, an edge case that produces an error - the deployment stops. You can also set expected outputs with tolerances, so you're testing not just "does it run" but "does it produce the right prices."

**Impact analysis is automatic.** When a new version is ready, Haute scores a portfolio sample through both the new model and the current one, and produces a report: how many quotes changed, by how much, which segments moved, what the overall premium impact looks like. It breaks down results by segment - vehicle fuel type, region, driver age band - so you can see exactly where the change hits hardest.

**The path to production is always the same.** Deploy to staging. Verify it works. Review the impact report. Approve the change. Promote to production. Every deployment records what changed, who approved it, the impact numbers, and what version it replaced.

Nobody can push a model to production from their laptop. The only path to live is through the review process.

---

## Deploy without learning deployment

Most pricing teams need IT to deploy a model change. That process can take weeks.

Haute handles deployment for you. When you're ready, your pipeline gets packaged, validated, and deployed as a live scoring API - a service that returns a price when it receives a quote. You choose the target once during setup:

| Target | How it works |
|---|---|
| **Databricks** | Packages your pipeline as an MLflow model and deploys it to a Databricks Model Serving endpoint |
| **Docker** | Builds a container image that runs anywhere |
| **Azure / AWS / GCP** | Builds and pushes to your cloud provider's container service |

You don't need to know what any of those words mean. The deployment infrastructure is set up once by your team's IT or engineering group. After that, every change you make follows the same automated path: validate, test, stage, review, promote.

A staging environment lets you verify the new version works before it touches production. Smoke tests hit the live endpoint with test quotes to make sure it responds correctly. If everything passes, promoting to production is a single step.

---

## Submodels keep complex structures manageable

Real pricing pipelines have dozens of steps. Frequency models, severity models, large loss loads, expense loads, NCD logic, territorial adjustments - the graph gets big.

Submodels let you group a set of nodes into a single collapsible block. In the main view, you see one clean node labelled "Frequency Model." Double-click it to see the internals. Click the breadcrumb to navigate back.

You can reuse a submodel across different pipelines - build your territorial adjustment once and reference it everywhere. If you change your mind, dissolve the submodel and the nodes expand back into the parent pipeline.

---

## The same pipeline does everything

Build it once. Use it five ways:

- **Live quoting** - a single request, sub-second response from your deployed endpoint
- **Batch scoring** - millions of rows, processed in parallel on your machine
- **What-if analysis** - change inputs and watch the price move through every step
- **Impact analysis** - compare a new model against the current one across your portfolio
- **Price tracing** - click any output and see exactly how it was calculated

No rebuilding, no re-exporting, no maintaining separate versions for different use cases.

---

## Built on open source, not instead of it

Haute doesn't re-invent machine learning, data processing, or deployment. It connects the best open-source tools that already exist and wraps them in a visual interface designed for pricing work.

Your models are standard Python - scikit-learn, CatBoost, LightGBM, XGBoost, or anything else that runs in Python. Your data processing uses Polars, the fastest dataframe library available. Your deployment uses MLflow, Docker, and cloud-native services that your engineering team already knows.

Everything is a Python file on disk. There is no proprietary format, no export step, no binary blob locked inside a platform. If you ever want to leave Haute, take your files and go. They're standard Python.

---

## What the visual editor looks like in practice

**The canvas** is a dark, minimal workspace with a subtle dot grid. Nodes have colour-coded accent stripes - blue for data sources, green for API inputs, purple for model scores, emerald for rating steps. Connections between nodes glow and animate when you're tracing a price.

**The node palette** sits on the left. Drag a node type onto the canvas to create it. The palette enforces the rules - you can only have one API Input, one Output, and one Live Switch per pipeline.

**The data preview** sits at the bottom. Click any node and its output data appears as a table with type-coloured column headers (integers in blue, decimals in green, text in amber). Row counts, column counts, and execution timing are always visible. Resize the panel by dragging. Collapse it when you need more canvas space.

**The code editor** (for Polars transform nodes) has line numbers, auto-indentation, bracket matching, block commenting, and line duplication. You write small pieces of data logic, not entire programs.

**Right-click any node** to rename it, duplicate it, create a reusable instance, or delete it. **Undo and redo** work across all operations, up to 100 steps. **Snap to grid** keeps your layout clean. **Auto-layout** arranges the entire pipeline automatically.

Every change saves to a Python file on disk. If a developer opens that file in their editor, the visual editor updates in real time. If they change the file, the visual editor reflects it instantly. Both views are always in sync.

---

## Free

Haute is free to use. Free for your team, your models, your infrastructure, your production workloads. There is no usage limit, no seat count, no premium tier.

Haute is licensed under the [GNU Affero General Public License v3.0](LICENSE). See [LICENSE](LICENSE) for details.

---

## Quick start

```bash
pip install haute
haute init my-project --target databricks --ci github
cd my-project
haute serve
```

`haute serve` opens the visual editor in your browser. From there, you're building.

---

## Architecture

| Layer | Technology |
|---|---|
| **Visual Editor** | React, TypeScript, React Flow |
| **Backend** | Python, FastAPI, Polars |
| **Models** | Any Python model (scikit-learn, CatBoost, LightGBM, XGBoost, etc.) |
| **Deploy Targets** | Databricks · Docker · Azure Container Apps · AWS ECS · GCP Cloud Run |
| **CI/CD** | GitHub Actions · GitLab CI · Azure DevOps |
