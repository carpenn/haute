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

Haute is a free, open-source pricing engine for insurance teams. Build rating pipelines in a visual editor, deploy them as live scoring APIs, and get safety rails and audit trails built in - without needing to learn Docker, Git, or cloud tooling.

---

## Quick start

```bash
pip install haute
haute init my-project --target databricks --ci github
cd my-project
haute serve
```

`haute serve` opens a browser-based visual editor. Drag, connect, and configure nodes - or hand the file to a developer and let them work in code. Both views stay in sync automatically.

---

## Visual editor with real code underneath

Haute gives you a drag-and-drop editor for building rating pipelines - data sources, transforms, model scores, rating steps, and outputs, connected as a flow diagram. Click any node to see its data.

The difference from other visual tools: **everything is a Python file on disk**. Change something in the editor and the code updates. Edit the code and the editor updates. There's no export step, no proprietary format, nothing locked inside a platform.

This means analysts can work visually while developers can work in code. Reviews, history, and rollback come for free because it's just a file. And if you ever want to leave Haute, your pipelines are standard Python - take them with you.

---

## Fast, even on large datasets

Haute uses [Polars](https://pola.rs/) under the hood - a modern data engine that's significantly faster than pandas. Pipelines process data in parallel automatically, so things like scoring a 600K-row portfolio or joining large rating tables happen in seconds, not minutes.

The editor is fast too. Clicking between nodes doesn't re-run the whole pipeline - Haute caches results intelligently, so previewing data at any point in your pipeline is near-instant.

Any model that runs in Python works: GLMs, CatBoost, LightGBM, XGBoost, or whatever your team uses.

---

## Connect to Databricks

Point a data source node at a Databricks Unity Catalog table - browse your catalogs, schemas, and tables directly from the editor. Click Fetch and the data downloads to your machine. After that, the pipeline runs locally at full speed without needing a live connection.

Even large tables (millions of rows) download efficiently - data streams in batches so your machine doesn't run out of memory. If a download fails halfway through, nothing is corrupted; just click Fetch again.

You can also write custom SQL if you only need a subset of the data.

---

## Trace any price

Click any output price and see exactly how it was calculated - the path through every node, with the value at each step:

```
base rate £300  →  area factor ×1.2  →  NCD ×0.85  →  frequency load ×1.35  →  £412.50
```

When you trace a specific column (say, the frequency load), Haute highlights only the nodes that contributed to that value. Everything else fades away so you can focus on what matters.

This is useful for debugging ("why is this price so high?"), for explaining decisions to underwriters, and for regulatory requirements like Solvency II, IFRS 17, and FCA pricing practices where you need to show how a price was derived.

---

## Submodels

As pipelines grow, you can group nodes into submodels - self-contained pieces that collapse into a single node in the main view. Select a set of nodes in the editor, click Group, and Haute creates a submodel automatically.

Double-click a submodel to see its internals. Ungroup it if you change your mind. Reuse the same submodel across different pipelines. It keeps complex rating structures manageable without losing visibility.

---

## Deployment

Haute deploys your pipeline as a live scoring API. You choose the target once during setup - Databricks, Azure, AWS, GCP, or a plain Docker container - and from there, getting to production is handled for you.

| Target | Status |
|---|---|
| **Databricks Model Serving** | Implemented |
| **Docker container** | Implemented |
| **Azure Container Apps** | Build+push implemented |
| **AWS ECS** | Build+push implemented |
| **GCP Cloud Run** | Build+push implemented |

**You don't need to know how any of this works.** Analysts only need Python installed on their machine. The deployment infrastructure runs in your team's CI system - all you do is make your changes and push them for review.

---

## Safety

One wrong factor can misprice an entire book before anyone notices. Haute is opinionated about preventing this.

**Before anything goes live**, Haute scores a set of test quotes through your pipeline. If anything breaks - a missing column, a model that won't load, an edge case that errors - the deployment stops. You can also set expected outputs with tolerances, so you're testing not just "does it run" but "does it produce the right prices".

**Impact analysis is automatic.** When a new version is ready, Haute scores a portfolio sample through both the new model and the current one, and produces a report: how many quotes changed, by how much, which segments moved, what the overall premium impact looks like. The people who need to approve the change see the financial effect before it goes live.

**The path to production is always the same**: deploy to staging, verify it works, review the impact, then manually promote. Every deployment records what changed, who approved it, the impact report, and what version it replaced. This covers the traceability requirements for Solvency II, IFRS 17, and FCA pricing practices without you having to build anything.

---

## One pipeline, multiple uses

The same pipeline works for:

- **Live quoting** - a single request, sub-second response
- **Batch scoring** - millions of rows, processed in parallel
- **What-if analysis** - change inputs and see how the price moves through each step
- **Impact analysis** - compare a new model against the current one across your portfolio
- **Price tracing** - click any output and see exactly how it was calculated

---

## Change management

Every change to a pipeline is tracked. Haute generates the automation for your team's workflow - GitHub, GitLab, or Azure DevOps - so that proposed changes are validated and tested automatically before anyone reviews them. Once approved, the new model goes to staging, gets verified, and an impact report is produced. Promoting to production is always a separate, deliberate step.

Nobody can push a model to production from their laptop. The only path to live is through the review process.

---

## Architecture

| Layer | Technology |
|---|---|
| **Visual Editor** | React, TypeScript, React Flow |
| **Backend** | Python, FastAPI, Polars |
| **Models** | Any Python model (scikit-learn, CatBoost, LightGBM, XGBoost, etc.) |
| **Deploy Targets** | Databricks · Docker · Azure Container Apps · AWS ECS · GCP Cloud Run |
| **CI/CD** | GitHub Actions · GitLab CI · Azure DevOps |

---

## License

Elastic License 2.0. Free to use for your team, your models, your infrastructure. The only restriction is that you can't sell Haute itself as a hosted service. See [LICENSE](LICENSE) for details.