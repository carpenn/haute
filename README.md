<div align="center">

# ✦ Haute

### Open-source pricing engine for insurance.

<br>

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Elastic License 2.0](https://img.shields.io/badge/license-Elastic_2.0-blue?style=flat-square)](LICENSE)
[![Databricks](https://img.shields.io/badge/deploy-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://databricks.com)
[![AWS](https://img.shields.io/badge/deploy-SageMaker-FF9900?style=flat-square&logo=amazonaws&logoColor=white)](https://aws.amazon.com/sagemaker/)
[![Azure](https://img.shields.io/badge/deploy-Azure_ML-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/machine-learning)
[![Docker](https://img.shields.io/badge/deploy-Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

</div>

---

Haute is a free pricing engine that lets insurance teams build rating pipelines in a visual editor, keep everything as version-controlled Python, and deploy to a live scoring API - on Databricks, AWS, Azure, or a plain Docker container.

It's aimed at the gap between enterprise platforms like Radar that cost six or seven figures a year, and the in-house Python notebooks that never quite make it to production.

---

## Why this exists

### The platform problem

Tools like WTW Radar have been the default for 30 years. They give you visual editing, deployment, monitoring - the full package. But you pay for it in ways beyond the licence fee:

Your models live inside a proprietary format. If you want to leave, you're rebuilding from scratch. Your team learns a platform-specific way of working instead of Python, Git, and the tools the rest of the data science world uses. You're tied to a single cloud provider. And there's no way to look under the hood - no source code, no public documentation, no community outside the vendor's ecosystem.

These tools made sense when there wasn't a credible open-source alternative. That's less true now.

### The notebook problem

The other path - building pricing pipelines in Python notebooks with custom deployment scripts - gives you freedom, but the deployment story is usually painful. Models sit in notebooks for months. Getting to a live API means weeks of back-and-forth with an engineering team. There's no visual layer for analysts to work with, no impact analysis before changes go live, and no standardised way of doing things across the team.

---

## What Haute actually does

A pricing pipeline in Haute is a Python file. Standard Python, using Polars DataFrames. It lives in Git, you can open it in any editor, and it's testable with pytest like any other code.

But it's also a visual graph. Haute runs a browser-based editor where the pipeline is rendered as a drag-and-drop flow diagram - nodes for data sources, transforms, models, and outputs, connected by edges that show how data moves through the rating structure. You can click into any node and see its output data.

The key bit: **these are the same thing**. The visual editor reads from and writes to the Python file. Change a node in the GUI and the `.py` file updates on disk. Edit the `.py` file and the graph updates in the browser. There's no import/export step, no translation layer, no "generate code" button. They're always in sync.

This means analysts can work visually while everything stays in version control. Technical people can work in code without losing the visual representation. And nobody has to choose one or the other.

---

## Deployment

Haute deploys pipelines as live scoring APIs. You set the target once in a config file, and the deployment handles pruning the pipeline to the scoring path, bundling model files, running validation, and pushing to the endpoint.

| Target | What gets deployed |
|---|---|
| **Databricks** | MLflow model on Databricks Model Serving |
| **AWS SageMaker** | MLflow model on a SageMaker real-time endpoint |
| **Azure ML** | MLflow model on an Azure ML managed endpoint |
| **Docker** | Self-contained container with a FastAPI server - runs anywhere |

The Docker target is worth calling out specifically. It has zero cloud dependencies. If you can run a container - on Kubernetes, ECS, on-prem, a laptop - you can deploy a Haute pipeline. It's there as a universal option for teams that don't want to be tied to any managed ML platform.

---

## Safety

This is where Haute is most opinionated. A pricing engine without safety rails is dangerous - one wrong factor can misprice an entire book before anyone notices. So deployment safety isn't optional, it's baked into how the tool works.

Before any model reaches production, Haute scores a set of example quotes through the pipeline. If anything breaks - a missing column, a model that won't load, a transform that errors on edge cases - the deployment is blocked. You can also set expected outputs with tolerances, so you're testing not just "does it run" but "does it produce the right prices".

When a new model is ready, it gets deployed to a staging environment first. Haute then scores a portfolio sample through both the new model and the one currently in production, and produces a comparison report: how many quotes changed, by how much, which segments moved, what the overall premium impact looks like. The people who need to approve the change see the financial effect before it goes live. Nobody has to ask "what will this do to the book" - the answer is already there.

The path to production is always the same: deploy to staging, verify it works, review the impact, then manually promote. A solo actuary gets the same process as a large team. You can adjust how many people need to sign off, but the structure itself is fixed. There are no shortcuts because there shouldn't be.

Every deployment records what changed, who approved it, the impact report, and what version it replaced. Solvency II, IFRS 17, FCA pricing practices - the traceability requirements are real, and this covers them without you having to build anything.

---

## Why Python matters here

Haute pipelines are standard Python. Not a proprietary DSL, not a visual-only format, not a configuration language that looks like code but isn't.

This matters for a few reasons. People you hire already know Python - or can learn it, and those skills are useful beyond one vendor's ecosystem. Any model that runs in Python works in Haute: scikit-learn GLMs, CatBoost, LightGBM, XGBoost, whatever comes next. You can read the pipeline file and understand what it does. And every node is a plain function, so standard testing tools just work.

The dataframe library is [Polars](https://pola.rs/) rather than pandas - it's faster, has a cleaner API, and handles the kind of operations pricing pipelines need (joins, group-bys, window functions) well. But the point is that it's a real, general-purpose data tool, not something that only exists inside Haute.

---

## One pipeline, multiple uses

The same pipeline file works for live quoting (a single JSON request, sub-second response), batch scoring (millions of rows), what-if analysis (an analyst dragging sliders to see how price changes through each node), impact analysis, and execution tracing.

On the tracing point - you can click any output price and see the path through every node that produced it, with intermediate values at each step. Something like:

```
base rate £300  →  area factor ×1.2  →  NCD ×0.85  →  frequency load ×1.35  →  £412.50
```

This is genuinely useful for regulatory explainability, but it's also just a good way to debug a pipeline and understand what's happening at each step.

---

## Version control and CI/CD

Pipelines are plain `.py` files in Git. This gives you branching (work on a new rating structure without touching production), pull requests (every change is reviewed and discussed before it goes live), full history, and the ability to roll back to any previous version.

Haute generates the automation around this when you set up a project. You tell it which CI provider your team uses and it creates the configuration files that wire everything together. Proposed changes get validated, tested, and scored automatically before anyone reviews them. Once a change is approved and merged, the new model goes to staging, gets verified, and an impact report is produced. Promoting to production is always a separate, deliberate act.

You can also lock things down so that deployments can only happen through this process - nobody can push a model to production from their laptop. The only path to live is through the review pipeline.

The intent is that pricing teams get the same release discipline that software engineering teams use, without having to build it themselves.

---

## Comparison

Being honest about this:

|  | Haute | WTW Radar | Earnix | In-House |
|---|---|---|---|---|
| **Cost** | Free | $100K–$1M+/yr | Enterprise | Engineering salaries |
| **Visual editor** | Yes | Yes (more mature) | Yes | No |
| **Code-native** | Python | Proprietary | Proprietary | Whatever you build |
| **Version control** | Git (built-in) | No | No | If you set it up |
| **Deploy targets** | Multi-cloud + Docker | Azure | Vendor-managed | DIY |
| **Impact analysis** | Built-in | Manual / partial | Partial | DIY |
| **Maturity** | Early | 30 years, 500+ clients | Established | Varies |
| **Vendor lock-in** | None | High | High | None |

Radar and Earnix are mature products with decades of development and large teams behind them. Haute is newer and less feature-complete. The trade-off is openness, portability, and cost. If you need the full breadth of what Radar offers today - optimisation engines, monitoring dashboards, commercial underwriting workflows - Haute isn't there yet. If you want to own your pricing code, deploy to your own infrastructure, and not be locked into a platform, it's worth a look.

---

## Architecture

| Layer | Technology |
|---|---|
| **Visual Editor** | React, TypeScript, React Flow |
| **Backend** | Python, FastAPI, Polars, WebSocket sync |
| **Models** | MLflow (registry, tracking, serving) |
| **Deploy Targets** | Databricks · AWS SageMaker · Azure ML · Docker |
| **CI/CD** | GitHub Actions · GitLab CI (Azure DevOps planned) |

---

## License

Elastic License 2.0. Free to use for your team, your models, your infrastructure. The only restriction is that you can't sell Haute itself as a hosted service. See [LICENSE](LICENSE) for details.