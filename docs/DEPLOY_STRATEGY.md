# Haute - Deployment Strategy

**Status:** Draft - refining before implementation  
**Scope:** Config design, CI/CD, deploy targets, safety gates  

---

## 1. Problem

A pricing team builds a pipeline in Haute. They need to:

1. **Deploy it** to a live scoring API (Databricks, AWS, Azure, Docker, etc.)
2. **Release safely** - every change is tested, reviewed, impact-analysed, and auditable
3. **Configure once** - deployment target, CI provider, and safety thresholds defined in version-controlled config

The insurance industry adds a hard constraint: **a single wrong factor can misprice millions of pounds of premium before anyone notices**. The release process must catch bugs, unintended impact, and compliance violations - without slowing teams to a crawl.

Haute's competitive edge: all of this works out of the box from `haute init`. No DevOps team required.

---

## 2. Design Principles

1. **`haute.toml` is the single source of truth** - everything about *what* gets deployed, *where*, and *with what safety checks* lives in one committed file
2. **Secrets never go in config** - credentials live in `.env` (local) or CI secrets (remote), never in `haute.toml`
3. **Deploy targets and CI providers are independent choices** - you can deploy to Databricks from GitHub Actions, or to Docker from Azure DevOps, or any combination
4. **Convention over configuration** - sensible defaults for everything; most teams only need to fill in target-specific credentials
5. **Merge to main = deploy** - production deployments are triggered by merging to the main branch, never by running a manual command in production

---

## 3. What `haute init` Generates

```
my-pricing-project/
├── main.py                      # Starter pipeline
├── haute.toml                   # Project + deploy + safety config
├── .env.example                 # Credentials template (committed)
├── .env                         # Actual secrets (gitignored)
├── data/                        # Local data files
├── tests/                       # Tests + JSON test payloads
│   ├── quotes/
│   │   └── example.json
├── .github/                     # CI/CD workflows (if provider=github)
│   └── workflows/
│       ├── ci.yml               # PR checks: lint, test, validate, impact
│       └── deploy.yml           # Merge-to-main: validate → deploy → smoke test
├── .gitignore
└── pyproject.toml               # Dependencies (haute added automatically)
```

For Azure DevOps users, `.github/workflows/` is replaced with `azure-pipelines.yml`.  
For GitLab users, `.gitlab-ci.yml`.

---

## 4. Configuration Design - `haute.toml`

### 4.1 Target-Specific Generation

`haute init --target <target> --ci <provider>` generates a `haute.toml` containing **only** the sections relevant to the chosen target. No commented-out blocks, no unused target sections. Clean, minimal, obvious.

#### Example: `haute init --target databricks --ci github`

```toml
[project]
name = "motor-pricing"
pipeline = "main.py"

[deploy]
target = "databricks"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.databricks]
experiment_name = "/Shared/haute/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"       # Small | Medium | Large
serving_scale_to_zero = true

[test_quotes]
dir = "tests/quotes"

[safety]
impact_dataset = "data/portfolio_sample.parquet"
max_single_quote_change_pct = 25.0
max_avg_change_pct = 10.0
block_on_threshold_breach = true

[safety.approval]
min_approvers = 2

[ci]
provider = "github"

[ci.staging]
endpoint_suffix = "-staging"

[ci.production]
require_approval = true
min_approvers = 2
```

#### Example: `haute init --target docker --ci azure-devops`

```toml
[project]
name = "motor-pricing"
pipeline = "main.py"

[deploy]
target = "docker"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.docker]
registry = ""                         # e.g. "ghcr.io/myorg", "123456.dkr.ecr.eu-west-1.amazonaws.com"
port = 8080
base_image = "python:3.11-slim"

[test_quotes]
dir = "tests/quotes"

[safety]
impact_dataset = "data/portfolio_sample.parquet"
max_single_quote_change_pct = 25.0
max_avg_change_pct = 10.0
block_on_threshold_breach = true

[safety.approval]
min_approvers = 2

[ci]
provider = "azure-devops"

[ci.staging]
endpoint_suffix = "-staging"

[ci.production]
require_approval = true
min_approvers = 2
```

#### Example: `haute init --target sagemaker --ci gitlab`

```toml
[project]
name = "motor-pricing"
pipeline = "main.py"

[deploy]
target = "sagemaker"
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[deploy.sagemaker]
region = "eu-west-1"
instance_type = "ml.m5.large"
initial_instance_count = 1

[test_quotes]
dir = "tests/quotes"

[safety]
impact_dataset = "data/portfolio_sample.parquet"
max_single_quote_change_pct = 25.0
max_avg_change_pct = 10.0
block_on_threshold_breach = true

[safety.approval]
min_approvers = 2

[ci]
provider = "gitlab"

[ci.staging]
endpoint_suffix = "-staging"

[ci.production]
require_approval = true
min_approvers = 2
```

### 4.2 All Target-Specific Sections (reference)

These are never in the same file - only the one matching `--target` is generated.

```toml
# ── Databricks ────────────────────────────────────────────────
[deploy.databricks]
experiment_name = "/Shared/haute/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"       # Small | Medium | Large
serving_scale_to_zero = true

# ── Docker ────────────────────────────────────────────────────
[deploy.docker]
registry = ""                         # e.g. "ghcr.io/myorg"
port = 8080
base_image = "python:3.11-slim"

# ── AWS SageMaker ─────────────────────────────────────────────
[deploy.sagemaker]
region = "eu-west-1"
instance_type = "ml.m5.large"
initial_instance_count = 1

# ── Azure ML ──────────────────────────────────────────────────
[deploy.azure-ml]
resource_group = ""
workspace_name = ""
instance_type = "Standard_DS3_v2"
instance_count = 1
```

### 4.3 What Goes Where - The Separation

| Setting | Where | Committed to git? | Example |
|---|---|---|---|
| Deploy target, model name, endpoint | `haute.toml [deploy]` | ✅ Yes | `target = "databricks"` |
| Target-specific infra config | `haute.toml [deploy.<target>]` | ✅ Yes | `catalog = "main"`, `instance_type = "ml.m5.large"` |
| Safety thresholds | `haute.toml [safety]` | ✅ Yes | `max_avg_change_pct = 10.0` |
| CI provider and pipeline shape | `haute.toml [ci]` | ✅ Yes | `provider = "github"` |
| CI workflow files | `.github/workflows/` etc. | ✅ Yes | `ci.yml`, `deploy.yml` |
| Workspace credentials | `.env` (local) | ❌ No | `DATABRICKS_TOKEN=dapi...` |
| Workspace credentials (CI) | CI secrets | ❌ No | GitHub repo secrets |
| Test quote payloads | `tests/quotes/*.json` | ✅ Yes | `single_policy.json` |
| Impact comparison dataset | `data/` | ✅ Yes | `portfolio_sample.parquet` |

### 4.4 Why One File, Not Many

We considered `deployment/databricks.yaml`, `deployment/docker.yaml` etc. but rejected it:

- **More files = more confusion** for pricing teams who aren't DevOps engineers
- All targets share the same top-level fields (`model_name`, `endpoint_name`)
- Only one target is active at a time (`[deploy].target`)
- `haute init` generates only the relevant target section - no dead config
- If a team needs to switch target, they edit the `[deploy]` section and add the new `[deploy.<target>]` section - or re-run `haute init` in a clean project

### 4.5 Credentials by Target

Each deploy target reads credentials from environment variables. `haute init` generates a `.env.example` containing **only** the credentials for the chosen target - not a master list of every possible provider.

#### `--target databricks` generates:
```bash
# Haute - Databricks credentials
# Copy to .env and fill in your values. .env is gitignored.
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net
DATABRICKS_TOKEN=your_token_here
```

#### `--target sagemaker` generates:
```bash
# Haute - AWS SageMaker credentials
# Copy to .env and fill in your values. .env is gitignored.
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=eu-west-1
SAGEMAKER_ROLE_ARN=arn:aws:iam::123456789012:role/SageMakerRole
```

#### `--target azure-ml` generates:
```bash
# Haute - Azure ML credentials
# Copy to .env and fill in your values. .env is gitignored.
AZURE_SUBSCRIPTION_ID=
AZURE_TENANT_ID=
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=
```

#### `--target docker` generates:
```bash
# Haute - Docker registry credentials (optional)
# Only needed if pushing to a private registry.
# Copy to .env and fill in your values. .env is gitignored.
DOCKER_USERNAME=
DOCKER_PASSWORD=
```

---

## 5. Deploy Targets

### 5.1 Target Architecture

All targets consume the same `ResolvedDeploy` dataclass (already implemented). The target-specific layer is thin - it only handles "put this model somewhere and give me a URL".

```
haute.toml ──→ DeployConfig ──→ resolve_config()
                                    │
                  target-agnostic   │  _pruner.py     prune graph
                                    │  _bundler.py    collect artifacts
                                    │  _schema.py     infer schemas
                                    │  _validators.py  run test quotes
                                    │  _scorer.py     dry-run scoring
                                    │
                                    ▼
                              ResolvedDeploy
                                    │
                  target-specific   │  dispatch on [deploy].target
                                    │
                    ┌────────────────┼────────────────┐────────────────┐
                    ▼                ▼                ▼                ▼
              _mlflow.py       _docker.py       _sagemaker.py    _azure_ml.py
              (databricks)     (container)      (aws)            (azure)
```

### 5.2 Target Summary

| Target | `[deploy].target` | Install extra | What it produces | Credential env vars |
|---|---|---|---|---|
| **Databricks** | `"databricks"` | `haute[databricks]` | MLflow model → Databricks Model Serving endpoint | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| **Docker** | `"docker"` | (none - core) | OCI container image with FastAPI server | `DOCKER_USERNAME`, `DOCKER_PASSWORD` (optional, for registry push) |
| **AWS SageMaker** | `"sagemaker"` | `haute[aws]` | MLflow model → SageMaker real-time endpoint | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `SAGEMAKER_ROLE_ARN` |
| **Azure ML** | `"azure-ml"` | `haute[azure]` | MLflow model → Azure ML managed online endpoint | `AZURE_SUBSCRIPTION_ID`, `AZURE_CLIENT_ID`, etc. |

### 5.3 Install Extras

```bash
# Databricks (current)
uv add "haute[databricks]"     # mlflow, databricks-sdk

# AWS
uv add "haute[aws]"            # mlflow, sagemaker, boto3

# Azure
uv add "haute[azure]"          # mlflow, azure-ai-ml, azure-identity

# Docker (no extra needed - uses only stdlib + Dockerfile generation)
uv add haute

# Multiple targets
uv add "haute[databricks,aws]"
```

### 5.4 Docker Target - The Universal Escape Hatch

Docker is special because it has **zero cloud dependencies**. It produces a self-contained container that runs anywhere - on-prem Kubernetes, ECS, Cloud Run, or a laptop.

```bash
# Set target = "docker" in haute.toml, then:
haute deploy
# → Builds: Dockerfile, docker-compose.yml, FastAPI app
# → Optionally pushes to a registry
```

The generated container:
- FastAPI app wrapping `score_graph()` with the same `/invocations` API format
- Baked-in pipeline graph, model files, and static data
- Health check endpoint at `/health`
- Configurable via environment variables
- No runtime dependency on Databricks, AWS, or Azure

This makes Docker the **foundation target** - if you can run a container, you can run Haute.

---

## 6. CI/CD Strategy

### 6.1 The Release Flow

```
feature branch ──→ PR ──→ merge to main ──→ deploy
       │              │              │
    local dev      CI gate      CD pipeline
       │              │              │
  haute lint       ruff check      validate (re-run all checks)
  haute test       mypy .              │
  haute run        pytest              ├─→ deploy staging (auto)
                   haute lint          │     smoke test
                   haute deploy        │
                     --dry-run         ├─→ [approval gate]
                   haute impact        │
                     (PR comment)      └─→ deploy production
```

### 6.2 CI Provider Support

| Provider | Config key | Generated file(s) | Status |
|---|---|---|---|
| **GitHub Actions** | `provider = "github"` | `.github/workflows/ci.yml`, `.github/workflows/deploy.yml` | v1 |
| **Azure DevOps** | `provider = "azure-devops"` | `azure-pipelines.yml` | v2 |
| **GitLab CI** | `provider = "gitlab"` | `.gitlab-ci.yml` | v2 |
| **None** | `provider = "none"` | No CI files generated | v1 |

All providers run the same logical steps - only the YAML syntax differs.

### 6.3 GitHub Actions - CI Workflow (`ci.yml`)

Runs on every PR to `main`:

| Job | What it runs | Blocks merge on failure? |
|---|---|---|
| **lint** | `ruff check .` + `ruff format --check .` | Yes |
| **typecheck** | `mypy .` | Yes |
| **test** | `pytest -v` | Yes |
| **pipeline-validate** | `haute lint` + `haute deploy --dry-run` | Yes |
| **impact** | `haute impact --base origin/main` → PR comment | Configurable (default: warning only) |

### 6.4 GitHub Actions - Deploy Workflow (`deploy.yml`)

Runs on push to `main` (i.e. after PR merge):

| Step | What it does | Environment |
|---|---|---|
| **validate** | Re-runs lint, typecheck, test, pipeline validation on merge commit | - |
| **deploy-staging** | `haute deploy` with `--endpoint-suffix "-staging"` | `staging` (GitHub environment) |
| **smoke-test** | Score test quotes against the live staging endpoint | - |
| **deploy-production** | `haute deploy` | `production` (GitHub environment, requires approval) |
| **tag** | Git tag with version: `deploy/v{model_version}` | - |

The staging→production promotion uses **GitHub environment protection rules**:
- `staging`: no protection (auto-deploys)
- `production`: requires N approvers (configurable in `haute.toml [ci.production]`)

### 6.5 Azure DevOps - Pipeline (`azure-pipelines.yml`)

Same logical flow, Azure syntax:

```yaml
trigger:
  branches:
    include: [main]

pr:
  branches:
    include: [main]

stages:
  - stage: CI              # runs on PR
    jobs: [lint, typecheck, test, pipeline_validate]

  - stage: DeployStaging   # runs on merge to main
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/main')
    jobs: [deploy_staging, smoke_test]

  - stage: DeployProd      # manual approval gate
    condition: succeeded()
    jobs: [deploy_production]
    # Uses Azure DevOps Environments with approval checks
```

Credentials use Azure DevOps **variable groups** or **service connections** instead of GitHub secrets - but the env var names are identical (`DATABRICKS_HOST`, etc.).

### 6.6 No Shortcuts - Every Team Gets the Full Pipeline

Insurance pricing carries risk regardless of team size. A solo actuary mispricing a book is just as dangerous as a large team doing it. Every `haute init` project gets the full pipeline:

```
merge to main → validate → deploy staging → smoke test → [approval gate] → deploy production
```

There is no "simplified flow". The staging environment exists to catch issues in a production-like setting before real money is at stake. The approval gate exists because deploying rates should always require a second pair of eyes.

Teams can adjust the `min_approvers` count in `haute.toml`, but the staging→approval→production structure is non-negotiable.

---

## 7. Safety Gates

### 7.1 Test Quotes - Golden File Testing

Already implemented. Every deploy scores `tests/quotes/*.json` through the pruned pipeline and blocks on failure.

**Enhancement needed:** Support for **expected outputs** - not just "it doesn't crash" but "this input produces this exact price". This is the golden file pattern:

```json
[
  {
    "input": {"VehPower": 7, "VehAge": 3, "DrivAge": 42, "Area": "C"},
    "expected": {"technical_price": 384.0, "premium": 548.57},
    "tolerance_pct": 0.01
  }
]
```

If the output drifts beyond `tolerance_pct`, the deploy is blocked with a diff showing exactly which quotes changed and by how much.

### 7.2 Impact Report - `haute impact`

The headline safety feature. Scores a portfolio sample through both the current branch and the base branch, then produces a comparison:

```
╔══════════════════════════════════════════════════╗
║  IMPACT REPORT - motor-pricing                   ║
║  Compared: feature/area-factors → main            ║
║  Dataset: 50,000 policies                         ║
╠══════════════════════════════════════════════════╣
║  Quotes changed:     12,847 (25.7%)               ║
║  Average change:     +2.3%                         ║
║  Max increase:       +18.7% (postcode SW1A)        ║
║  Max decrease:       -4.2% (postcode EH1)          ║
║  Premium impact:     +£1.2M annual (+1.8%)         ║
║                                                    ║
║  ⚠ 847 quotes changed by >10%                     ║
║  ⚠ Segment "young_drivers" avg change +8.1%       ║
╚══════════════════════════════════════════════════╝
```

In CI, this is posted as a PR comment. Reviewers see the pricing impact before approving.

Threshold enforcement comes from `haute.toml [safety]`:
- `max_single_quote_change_pct` - any individual quote
- `max_avg_change_pct` - portfolio average
- `block_on_threshold_breach` - hard block vs. warning

### 7.3 Approval Gates

Configured in `haute.toml [safety.approval]` and enforced via CI provider:

| Provider | How approval is enforced |
|---|---|
| **GitHub** | Branch protection rules + environment protection rules |
| **Azure DevOps** | Environment approval checks |
| **GitLab** | Protected environments with required approvals |

`haute init` can generate the branch protection config (GitHub) or print setup instructions for other providers.

### 7.4 Rollback

```bash
haute rollback motor-pricing           # revert to previous model version
haute rollback motor-pricing --to 3    # revert to specific version
```

For Databricks: updates the serving endpoint to point to the previous model version (instant, no redeployment).  
For Docker: re-tags the previous image as `latest` and triggers a redeploy.  
For SageMaker/Azure ML: updates the endpoint to the previous model version.

On git: creates a revert commit on main, which triggers the normal deploy flow.

### 7.5 Audit Trail

Every deployment produces a record (stored in the deploy manifest + MLflow run metadata):

```json
{
  "pipeline": "motor-pricing",
  "version": 3,
  "git_sha": "a1b2c3d",
  "git_branch": "main",
  "deployed_by": "ci/github-actions",
  "timestamp": "2026-02-16T15:30:00Z",
  "impact_report_sha": "e4f5g6h",
  "test_quotes_passed": true,
  "pr_number": 47,
  "pr_approvers": ["jane", "alex"],
  "previous_version": 2,
  "target": "databricks",
  "endpoint": "motor-pricing"
}
```

---

## 8. Decisions Made

### D1: `haute init` is flag-driven, generates only relevant files

```bash
haute init                                    # defaults: databricks + github
haute init --target docker --ci azure-devops  # Docker + Azure DevOps
haute init --target sagemaker --ci gitlab     # AWS + GitLab
haute init --target docker --ci none          # Docker, no CI (manual deploy only)
```

- Only the chosen target's `[deploy.<target>]` section appears in `haute.toml`
- Only the chosen target's credentials appear in `.env.example`
- Only the chosen CI provider's workflow files are generated
- No commented-out blocks, no unused sections

### D2: Every team gets staging → approval gate → production

No simplified flow. Insurance pricing carries risk regardless of team size. The full pipeline is non-negotiable:

```
merge to main → validate → deploy staging → smoke test → [approval gate] → deploy production
```

### D3: `HAUTE_` env var overrides for environment-specific config

Staging and production may need different infra settings (e.g. workload size). Rather than duplicating config across files, use environment variables:

```
Resolution order (highest wins):
  1. CLI flags          --model-name foo
  2. Environment vars   HAUTE_MODEL_NAME=foo
  3. haute.toml         model_name = "foo"
  4. Auto-detection     pipeline file stem
```

CI workflow sets `HAUTE_SERVING_WORKLOAD_SIZE=Medium` in the production job's environment. The `haute.toml` value is the base (staging default), and env vars override for production.

### D4: `haute.toml` says *what*, workflow file says *how*

- `haute.toml [ci]` declares intent: "this project uses GitHub Actions"
- The generated workflow file contains the actual YAML steps
- `haute init` generates the workflow file **once** - teams own it from that point
- No re-generation on config changes (avoids the "managed file you can't customise" antipattern)

### D6: Same target for staging and production

Staging and production always use the same deploy target. If you deploy to Databricks, both staging and production are Databricks endpoints. No mixing Docker staging with Databricks production.

This keeps the deployment pipeline simple and ensures staging is a true replica of production. The only differences between staging and production are:

- **Endpoint name** - staging gets the `-staging` suffix (e.g. `motor-pricing-staging`)
- **Infra overrides** - production may use larger workload sizes via `HAUTE_` env vars
- **Approval gate** - production requires human approval, staging auto-deploys

```toml
[deploy]
target = "databricks"          # one target, used for both staging and production
model_name = "motor-pricing"
endpoint_name = "motor-pricing"

[ci.staging]
endpoint_suffix = "-staging"   # deploys to "motor-pricing-staging"

[ci.production]
require_approval = true
min_approvers = 2              # deploys to "motor-pricing"
```

### D8: Solo mode is a config change, not a CLI flag

`haute init` always generates team defaults (`min_approvers = 2`). A solo developer edits two values in `haute.toml`:

```toml
[safety.approval]
min_approvers = 0

[ci.production]
require_approval = false
min_approvers = 0
```

And skips adding protection rules to the `production` GitHub environment.

The full pipeline still runs identically - validate → staging → smoke test → production. The only difference is there's no pause for human approval. When the team grows, bump the values back up and add environment protection rules. No workflow regeneration needed.

### D9: `pyproject.toml` extras

```
haute[databricks]   → mlflow, databricks-sdk      (existing, keep as-is)
haute[catboost]     → catboost                     (existing, keep as-is)
haute[aws]          → mlflow, sagemaker, boto3     (new)
haute[azure]        → mlflow, azure-ai-ml, azure-identity  (new)
```

Docker target needs no extra - it generates a Dockerfile using only stdlib.

---

## 9. No Open Questions

All design questions have been resolved. See §8 Decisions Made.

---

## 10. Implementation Order

| Phase | What | Depends on |
|---|---|---|
| **Now** | Finalise this design doc, resolve open questions | - |
| **P1** | Expand `haute.toml` schema with `[safety]` and `[ci]` sections | Design finalised |
| **P2** | `haute init` generates CI/CD workflow files based on `[ci].provider` | P1 |
| **P3** | `haute impact` command + PR comment integration | P1 |
| **P4** | Golden file test quotes (expected outputs with tolerance) | Existing tests/quotes infra |
| **P5** | Docker deploy target (`_docker.py`) | Existing `ResolvedDeploy` |
| **P6** | AWS SageMaker deploy target (`_sagemaker.py`) | P5 pattern |
| **P7** | Azure ML deploy target (`_azure_ml.py`) | P5 pattern |
| **P8** | `haute rollback` command | Deploy targets implemented |
| **P9** | Azure DevOps + GitLab CI templates | P2 pattern |

---

## 11. Summary

- **One config file** (`haute.toml`) defines project, deploy target, safety thresholds, and CI shape
- **Secrets in env vars** - `.env` locally, CI secrets remotely, `HAUTE_` prefix for overrides
- **Deploy targets are pluggable** - all share `ResolvedDeploy`, only the final push differs
- **Docker is the universal target** - zero cloud dependencies, runs anywhere
- **Merge to main = deploy** - every change is tested, reviewed, and impact-analysed before production
- **`haute init --target <t> --ci <p>` scaffolds everything** - only the relevant config, credentials, and CI/CD workflows for the chosen target and provider
- **No shortcuts** - every team gets staging → approval gate → production, regardless of size
