# Databricks Setup Guide

How to set up a Databricks workspace for deploying Haute pipelines as live scoring APIs.

---

## 1. Get a Databricks Workspace

If your organisation already has a Databricks workspace, skip to [step 2](#2-create-a-personal-access-token). Otherwise:

### Azure Databricks (most common for UK insurance)

1. Go to the [Azure Portal](https://portal.azure.com)
2. Search for **Azure Databricks** → click **Create**
3. Fill in:
   - **Subscription** — your Azure subscription
   - **Resource group** — create one or use existing (e.g. `rg-pricing`)
   - **Workspace name** — e.g. `dbw-pricing-dev`
   - **Region** — e.g. `UK South`
   - **Pricing tier** — **Premium** (required for Unity Catalog and Model Serving)
4. Click **Review + Create** → **Create**
5. Once deployed, click **Go to resource** → **Launch Workspace**
6. Your workspace URL will look like: `https://adb-1234567890123456.12.azuredatabricks.net`

### AWS Databricks

1. Go to [accounts.cloud.databricks.com](https://accounts.cloud.databricks.com)
2. Create a workspace in your preferred AWS region
3. Your workspace URL will look like: `https://dbc-abc12345-1234.cloud.databricks.com`

### GCP Databricks

1. Go to [accounts.gcp.databricks.com](https://accounts.gcp.databricks.com)
2. Create a workspace in your preferred GCP region

---

## 2. Create a Personal Access Token

Haute uses a Databricks Personal Access Token (PAT) to authenticate. To create one:

1. Open your Databricks workspace in a browser
2. Click your **user icon** (top-right) → **Settings**
3. Go to **Developer** → **Access tokens**
4. Click **Manage** → **Generate new token**
5. Set:
   - **Comment** — e.g. `haute-deploy`
   - **Lifetime** — 90 days (or your organisation's policy)
6. Click **Generate** → **copy the token immediately** (you won't see it again)

The token looks like: `dapi_your_token_here`

### Store it in `.env`

```bash
cd your_project/
cp .env.example .env
```

Edit `.env`:

```bash
DATABRICKS_HOST=https://adb-1234567890123456.12.azuredatabricks.net
DATABRICKS_TOKEN=dapi_your_token_here
```

> **Security:** `.env` is gitignored by default. Never commit tokens to git.

---

## 3. Set Up Unity Catalog

Unity Catalog is where your deployed model is registered. You need a **catalog** and a **schema**.

### Check if Unity Catalog is enabled

1. In your Databricks workspace, click **Catalog** in the left sidebar
2. If you see a catalog browser, Unity Catalog is enabled
3. If not, ask your workspace admin to enable it

### Create a catalog (if needed)

Most workspaces already have a `main` catalog. If you need a new one:

```sql
-- Run in a Databricks SQL editor or notebook
CREATE CATALOG IF NOT EXISTS main;
```

### Create a schema for your models

```sql
CREATE SCHEMA IF NOT EXISTS main.pricing;
```

### Update `haute.toml`

```toml
[deploy.databricks]
catalog = "main"
schema = "pricing"
```

---

## 4. Set Up an MLflow Experiment

Haute logs each deployment as an MLflow run. You need an experiment to hold these runs.

### Option A: Let Haute create it automatically

If the experiment doesn't exist, MLflow will create it on first deploy. Just set the path in `haute.toml`:

```toml
[deploy.databricks]
experiment_name = "/Shared/haute/motor-pricing"
```

### Option B: Create it manually

1. In Databricks, click **Experiments** in the left sidebar (under Machine Learning)
2. Click **Create Experiment**
3. Set:
   - **Name** — `/Shared/haute/motor-pricing`
   - **Artifact Location** — leave default (dbfs)
4. Click **Create**

### Naming convention

We recommend `/Shared/haute/<model-name>` so all team members can access it:

```
/Shared/haute/motor-pricing
/Shared/haute/home-pricing
/Shared/haute/commercial-property
```

---

## 5. Enable Model Serving

Databricks Model Serving hosts your pipeline as a REST API. To verify it's available:

1. Click **Serving** in the left sidebar
2. If you see the Serving page, it's enabled
3. If not, ask your workspace admin — Model Serving requires a **Premium** tier workspace

> **Note:** Model Serving is billed per compute-second. Setting `serving_scale_to_zero = true` in `haute.toml` means you only pay when the endpoint receives requests.

---

## 6. Configure `haute.toml`

Here's a complete example with all the Databricks settings:

```toml
[project]
name = "motor-pricing"
pipeline = "pipelines/my_pipeline.py"

[deploy]
target = "databricks"
model_name = "motor-pricing"              # Name in MLflow Model Registry
endpoint_name = "motor-pricing"           # URL slug for the serving endpoint

[deploy.databricks]
experiment_name = "/Shared/haute/motor-pricing"
catalog = "main"
schema = "pricing"
serving_workload_size = "Small"           # Small | Medium | Large
serving_scale_to_zero = true              # Scale to zero when idle

[test_quotes]
dir = "test_quotes"
```

### Where each value comes from

| Setting | Where to find it |
|---|---|
| `DATABRICKS_HOST` | Your workspace URL (step 1) — e.g. `https://adb-xxx.12.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal access token (step 2) — starts with `dapi` |
| `catalog` | Unity Catalog name (step 3) — usually `main` |
| `schema` | Unity Catalog schema (step 3) — e.g. `pricing` |
| `experiment_name` | MLflow experiment path (step 4) — e.g. `/Shared/haute/motor-pricing` |
| `model_name` | You choose this — it's the name in the Model Registry |
| `endpoint_name` | You choose this — it becomes part of the API URL |
| `serving_workload_size` | `Small` for dev/test, `Medium` or `Large` for production |

---

## 7. Deploy

### Dry run first

Validate everything without deploying:

```bash
haute deploy --dry-run
```

This will:
- Parse your pipeline
- Prune to the scoring path
- Collect model artifacts
- Score all test quotes in `test_quotes/`
- Report any errors

### Deploy for real

```bash
haute deploy
```

Output:

```
  ✓ Loaded config from haute.toml
  ✓ Parsed pipeline (12 nodes, 14 edges)
  ✓ Pruned to output ancestors (5 nodes)
  ✓ Collected 2 artifacts (freq.cbm, sev.cbm)
  ✓ Inferred input schema (10 columns)
  ✓ Test quotes: single_policy.json     1 rows  ok  (18ms)
  ✓ Test quotes: batch_policies.json    5 rows  ok  (24ms)
  ✓ Logged MLflow model: motor-pricing v1
  ✓ Model URI: models:/motor-pricing/1
```

### Check status

```bash
haute status motor-pricing
```

---

## 8. Call the API

Once the endpoint is live, call it with any HTTP client:

### curl

```bash
curl -X POST \
  "https://<workspace-url>/serving-endpoints/motor-pricing/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dataframe_records": [
      {"IDpol": 99001, "VehPower": 7, "DrivAge": 42, "Area": "C", "VehBrand": "B12"}
    ]
  }'
```

### Python

```python
import requests

url = "https://adb-xxx.12.azuredatabricks.net/serving-endpoints/motor-pricing/invocations"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}
payload = {
    "dataframe_records": [
        {"IDpol": 99001, "VehPower": 7, "DrivAge": 42, "Area": "C", "VehBrand": "B12"}
    ]
}

response = requests.post(url, json=payload, headers=headers)
print(response.json())
```

### Databricks SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="motor-pricing",
    dataframe_records=[
        {"IDpol": 99001, "VehPower": 7, "DrivAge": 42, "Area": "C", "VehBrand": "B12"}
    ],
)
print(response.predictions)
```

---

## Troubleshooting

### "No module named 'haute'"

The deployed model needs `haute` on PyPI. Make sure you've published:

```bash
pip install haute
```

### "PERMISSION_DENIED" on deploy

Your token needs these permissions:
- **Can Manage** on the MLflow experiment
- **USE CATALOG** and **USE SCHEMA** on Unity Catalog
- **Can Manage** on serving endpoints (or ask an admin to create the endpoint)

### "Endpoint not found" when calling the API

After `haute deploy`, the model is registered but the serving endpoint may take 5-10 minutes to provision. Check status:

```bash
haute status motor-pricing
```

Or in the Databricks UI: **Serving** → look for your endpoint.

### Token expired

Tokens have a lifetime (default 90 days). Generate a new one and update `.env`:

```bash
DATABRICKS_TOKEN=dapi_new_token_here
```

### Scale-to-zero cold starts

With `serving_scale_to_zero = true`, the first request after idle may take 30-60 seconds. Set to `false` for production endpoints that need consistent latency.

---

## Cost Guide

| Component | Cost driver |
|---|---|
| **Model Serving** | Per compute-second while endpoint is active. Scale-to-zero eliminates idle cost. |
| **MLflow** | Free (included in workspace). Storage for artifacts is minimal. |
| **Unity Catalog** | Free (included in Premium tier). |

For a `Small` workload with scale-to-zero, expect **< £5/month** for a dev/test endpoint with occasional traffic.

---

## Checklist

Before your first deploy, confirm:

- [ ] Databricks workspace URL (`DATABRICKS_HOST` in `.env`)
- [ ] Personal access token (`DATABRICKS_TOKEN` in `.env`)
- [ ] Unity Catalog enabled with catalog + schema
- [ ] MLflow experiment path set in `haute.toml`
- [ ] Model Serving available in your workspace
- [ ] At least one test quote JSON in `test_quotes/`
- [ ] `haute deploy --dry-run` passes
