<div align="center">

# ✦ Haute

### The open-source pricing engine for insurance.

Build pricing pipelines visually. Deploy them anywhere. Own the code forever.

<br>

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Elastic License 2.0](https://img.shields.io/badge/license-Elastic_2.0-blue?style=flat-square)](LICENSE)
[![Databricks](https://img.shields.io/badge/deploy-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![AWS](https://img.shields.io/badge/deploy-SageMaker-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/sagemaker/)
[![Azure](https://img.shields.io/badge/deploy-Azure_ML-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/machine-learning)
[![Docker](https://img.shields.io/badge/deploy-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

## The Pricing Industry Has a Problem

Insurance pricing sits at a crossroads. Teams are caught between **enterprise platforms** that cost six or seven figures a year and **ad hoc scripts** that never make it to production.

### The legacy platform trap

Tools like WTW Radar have dominated for 30 years. They offer visual editing, model deployment, and monitoring — but at a price:

- **Vendor lock-in** — models built inside the platform don't leave it. Your IP lives in someone else's proprietary format.
- **Opaque pricing** — $100K to $1M+ per year in licensing alone, before implementation, training, and ongoing consulting.
- **Proprietary skills** — your team learns a platform-specific language instead of building transferable, industry-standard capabilities.
- **Single-cloud dependency** — locked to one cloud provider's infrastructure.
- **Closed ecosystem** — no open community, no public documentation, no way to inspect what the platform is actually doing under the hood.

These platforms were groundbreaking in the 1990s. But the world has moved on. Python is the lingua franca of data science. Git is how teams collaborate. Cloud infrastructure is a commodity. The pricing industry hasn't kept up.

### The DIY trap

The alternative — building everything in-house with Python notebooks and custom deployment scripts — gives you flexibility but none of the guardrails:

- **No visual editing** — analysts lose the drag-and-drop workflow they rely on for building and communicating rating structures.
- **No deployment path** — models stall in notebooks. Getting a pipeline to a live API takes weeks of DevOps work.
- **No safety rails** — no impact analysis, no approval gates, no audit trail. One wrong factor can misprice millions of pounds of premium before anyone notices.
- **No standardisation** — every team reinvents the wheel. No shared structure, no consistency, no institutional knowledge.

WTW themselves have said that insurers who've tried to build their own solutions have found them *"astronomically expensive to maintain"*. They're right. But the answer isn't to hand your pricing IP to a vendor. **The answer is better tooling.**

---

## What Haute Does Differently

Haute is a free, open-source pricing engine that gives you the best of both worlds — **visual tooling with the power and freedom of code**.

### The core idea

Your pricing pipeline is a Python file. It's a real file on your computer, version-controlled in Git, reviewable in a pull request, testable with standard tools. But it's also a **visual graph** — a drag-and-drop editor in your browser where you can see the entire rating structure, click into any node, and watch data flow through the pipeline in real time.

Edit either one. They stay in sync. Always.

This isn't "export to code" or "import from code". The visual editor and the Python file are **the same thing**, viewed two ways. Change a node in the GUI and the Python file updates on disk instantly. Change the Python file in your editor and the graph updates in the browser. No export step, no translation layer, no drift.

### Why this matters

- **Analysts** get the visual, interactive experience they're used to — without being trapped in a proprietary platform.
- **Technical teams** get real Python, real version control, real CI/CD — without losing the visual layer that makes pricing structures communicable.
- **Leadership** gets full audit trails, impact analysis, and deployment safety — without a seven-figure platform license.

---

## Deploy Anywhere

Haute doesn't lock you to a single cloud. Your pipeline deploys as a standard, portable model — the same scoring logic, the same API contract — to whichever infrastructure your organisation already uses.

| Target | What you get |
|---|---|
| **Databricks** | MLflow model → Databricks Model Serving endpoint |
| **AWS SageMaker** | MLflow model → SageMaker real-time endpoint |
| **Azure ML** | MLflow model → Azure ML managed online endpoint |
| **Docker** | Self-contained container → runs on any infrastructure (Kubernetes, ECS, on-prem, laptop) |

Docker is the universal fallback — zero cloud dependencies, runs anywhere you can run a container.

You choose your deployment target once, in configuration. From that point on, the entire workflow — validation, staging, approval, production — is handled for you.

---

## Built-In Safety — Because Pricing Carries Risk

Most pricing tools treat deployment as a feature. Haute treats it as a **risk management problem**.

A single wrong rating factor can misprice an entire book before anyone notices. Haute's deployment pipeline is designed around this reality:

### Every change is tested before it goes live

Before any deployment, Haute scores your test quotes — real example requests — through the pipeline. If anything breaks, the deployment is blocked. You can also define **expected outputs with tolerances**: not just "it doesn't crash", but "this input produces this price, within 1%".

### Every change shows its impact

Before merging a pricing change, Haute scores a portfolio sample through both the old and new pipeline, and produces an **impact report**: how many quotes changed, by how much, which segments are affected, what the annual premium impact looks like. This is posted directly to your pull request so reviewers can see the financial impact before they approve.

### Every change goes through staging first

Every deployment follows the same path: **staging → smoke test → approval gate → production**. There is no shortcut. A solo actuary gets the same pipeline as a 50-person team. Insurance pricing carries risk regardless of team size.

### Every change is auditable

Every deployment records who approved it, what PR it came from, what git commit it points to, what the impact report showed, and what version it replaced. Full regulatory traceability — Solvency II, IFRS 17, FCA requirements — without building anything custom.

---

## Real Python. Transferable Skills. No Walled Garden.

Haute doesn't invent a new language. Every pricing pipeline is standard Python using [Polars](https://pola.rs/) DataFrames — the fastest dataframe library available.

What this means in practice:

- **Your team's skills transfer.** Everything they learn building pricing pipelines in Haute — Python, Polars, Git, CI/CD — is valuable everywhere. They're not learning a proprietary tool that only exists inside one vendor's ecosystem.
- **Any model works.** GLMs from scikit-learn, gradient boosting from CatBoost or LightGBM, deep learning from PyTorch — if it runs in Python, it runs in Haute. No ONNX conversion, no PMML export, no compatibility layer.
- **Everything is inspectable.** The pipeline is a file you can read. The models are files you can load. There is no black box.
- **Testing is native.** Every node in your pipeline is a plain Python function. Standard testing tools work out of the box.

---

## One Pipeline, Every Context

The same pipeline code — the exact same file — runs in every context:

| Context | What happens |
|---|---|
| **Live quoting** | A single JSON request comes in, gets scored through the pipeline, returns a price. Sub-second. |
| **Batch scoring** | A million-row dataset goes through the same pipeline. Same logic, same code path. |
| **What-if analysis** | An analyst adjusts input variables with sliders and watches the price update through every node in real time. |
| **Impact analysis** | The pipeline scores a portfolio sample to measure the effect of a proposed change before it goes live. |
| **Regulatory trace** | Click any output price and trace it back through every node — see the intermediate value at each step. |

No separate "batch mode" and "API mode". No reimplementation. One source of truth.

---

## Git-Native by Design

Every pricing pipeline is a plain text file, stored in Git. This unlocks the entire modern software engineering workflow that other industries take for granted:

- **Branching** — work on a new rating structure without touching production.
- **Pull requests** — every pricing change is reviewed, discussed, and approved before it goes live. With Haute's impact report posted directly to the PR.
- **History** — see exactly what changed, when, and who approved it. Roll back to any previous version instantly.
- **Collaboration** — multiple analysts can work on different parts of the pipeline simultaneously, on separate branches, and merge when ready.

No proprietary file formats. No binary blobs. No "who changed what and when?" mysteries. Just files, diffs, and reviews.

---

## CI/CD Out of the Box

Haute generates your entire continuous integration and deployment pipeline when you start a project. You choose your CI provider — **GitHub Actions**, **Azure DevOps**, or **GitLab CI** — and Haute creates the workflow files for you.

Every pull request automatically runs:

- **Linting** — catches code quality issues
- **Validation** — ensures the pipeline is structurally sound
- **Test quotes** — scores example requests to verify correctness
- **Impact analysis** — measures the financial effect of the change

Every merge to the main branch automatically:

- **Deploys to staging** — a production-like environment for final verification
- **Runs smoke tests** — scores test quotes against the live staging endpoint
- **Waits for approval** — a human must sign off before production
- **Deploys to production** — the live endpoint is updated

This is the same release discipline used by teams deploying financial trading systems, medical devices, and safety-critical software. Haute makes it the default for pricing.

---

## Visual Execution Trace — Regulatory Explainability

Click any output price in the data preview and Haute highlights the path through every node that produced it, showing the intermediate value at each step:

```
base rate £300  →  area factor ×1.2  →  NCD ×0.85  →  frequency load ×1.35  →  £412.50
```

Traced visually through the pipeline graph, with each node lit up and its contribution shown.

This is what regulators ask for. Solvency II, IFRS 17, and FCA pricing practices all require the ability to explain how a price was derived. Most teams build this manually, after the fact, as a compliance exercise. In Haute, it's a feature of the pipeline itself.

---

## Who Haute Is For

**Pricing teams at insurers of any size** who want to own their pricing IP, deploy with confidence, and stop paying platform rent.

- **Heads of Pricing** — full audit trail, impact analysis, deployment safety, and regulatory traceability. No vendor lock-in. No seven-figure license.
- **Pricing Analysts** — a visual editor that feels familiar, with the full power of Python underneath. Build, test, and iterate faster than in any legacy tool.
- **Data Scientists** — real Python, real models, real deployment. Your work doesn't stall in a notebook.
- **Actuarial Teams** — every pricing change is reviewed, impact-tested, and approved before production. The discipline your work demands, automated.

---

## How Haute Compares

|  | Haute | WTW Radar | Earnix | In-House Scripts |
|---|---|---|---|---|
| **Cost** | Free | $100K–$1M+/yr | Enterprise | Dev team salaries |
| **Visual editor** | ✅ | ✅ | ✅ | ❌ |
| **Code-native** | ✅ Real Python | ❌ Proprietary | ❌ Proprietary | ✅ |
| **Version control** | ✅ Git | ❌ | ❌ | ✅ (if disciplined) |
| **Deployment** | ✅ Multi-cloud + Docker | Azure only | Vendor-managed | ❌ DIY |
| **Impact analysis** | ✅ Built-in | Manual | Partial | ❌ |
| **Vendor lock-in** | None | High | High | None |
| **Open source** | ✅ | ❌ | ❌ | N/A |
| **Skills transferable** | ✅ Python + Git | ❌ Platform-specific | ❌ Platform-specific | ✅ |

---

## Architecture

| Layer | Technology |
|---|---|
| **Visual Editor** | React, TypeScript, React Flow |
| **Backend** | Python, FastAPI, Polars, WebSocket sync |
| **Models** | MLflow (registry, tracking, serving) |
| **Deploy Targets** | Databricks · AWS SageMaker · Azure ML · Docker |
| **CI/CD** | GitHub Actions · Azure DevOps · GitLab CI |
| **Safety** | Impact analysis · test quotes · staging gates · audit trail |

---

## Docs

Detailed documentation lives in [`docs/`](docs/):

- **[Architecture & Roadmap](ARCHITECTURE.md)** — system design, technical decisions, phased plan
- **[Deployment Strategy](docs/DEPLOY_STRATEGY.md)** — deploy targets, CI/CD, safety gates
- **[Databricks Setup](docs/DATABRICKS_SETUP.md)** — configuring your Databricks workspace

---

## License

Elastic License 2.0 — **free to use** for your pricing team, your models, your infrastructure. You own everything you build. The only restriction: you can't sell Haute itself as a hosted service. See [LICENSE](LICENSE) for full terms.