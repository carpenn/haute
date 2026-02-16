# Bug: `haute deploy` logs model to local MLflow instead of Databricks — missing tracking URI and Unity Catalog configuration

## Summary

When `[deploy] target = "databricks"` is configured in `haute.toml`, `haute deploy` successfully validates the pipeline and logs the model, but it registers the model in a **local SQLite-backed MLflow store** (`mlflow.db`) instead of the remote Databricks workspace. The deploy function never calls `mlflow.set_tracking_uri("databricks")` or `mlflow.set_registry_uri("databricks-uc")`, so MLflow defaults to a local backend.

Additionally, if the configured `experiment_name` path (e.g. `/Shared/haute/haute-test`) contains parent directories that don't yet exist in the Databricks workspace, the deploy fails with `RESOURCE_DOES_NOT_EXIST` rather than creating them automatically.

## Environment

| Component | Version |
|-----------|---------|
| **haute** | 0.1.10 |
| **mlflow** | 3.9.0 |
| **Python** | 3.13.7 |
| **OS** | Ubuntu Linux |

## Steps to Reproduce

1. Install `haute==0.1.10` with `mlflow>=3.0.0`.
2. Configure `haute.toml` with a Databricks deploy target:
   ```toml
   [deploy]
   target = "databricks"
   model_name = "haute-test"
   endpoint_name = "haute-test"

   [deploy.databricks]
   experiment_name = "/Shared/haute/haute-test"
   catalog = "workspace"
   schema = "default"
   ```
3. Configure `.env` with valid Databricks credentials:
   ```
   DATABRICKS_HOST=https://dbc-xxxxx.cloud.databricks.com
   DATABRICKS_TOKEN=dapi...
   ```
4. Run `haute deploy`.

### Expected behavior

The model is registered in the Databricks Unity Catalog as `workspace.default.haute-test` and is visible in the Databricks MLflow UI.

### Actual behavior

The model is registered in a local `mlflow.db` SQLite database in the project directory. The deploy appears to succeed, but the model is not visible in Databricks. The final output says:

```
  ✓ Logged MLflow model: haute-test v1
  ✓ Model URI: models:/haute-test/1

Deploy complete. Serve locally with:
  mlflow models serve -m "models:/haute-test/1" -p 5001
```

The clue is the `alembic.runtime.migration` lines in the output — they show SQLite migrations running locally, which should not happen when targeting a remote Databricks workspace.

## Root Cause

### Issue 1: Missing `mlflow.set_tracking_uri("databricks")`

In `haute/deploy/_mlflow.py`, the `deploy_to_mlflow()` function never configures the MLflow tracking URI. The `.env` credentials (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`) are loaded into `os.environ` by `_config.py:_load_env()`, but MLflow needs to be explicitly told to use the `"databricks"` tracking backend:

```python
# haute/deploy/_mlflow.py — deploy_to_mlflow()
def deploy_to_mlflow(resolved: ResolvedDeploy) -> DeployResult:
    import mlflow

    config = resolved.config
    model_name = config.model_name

    # ❌ Missing: mlflow.set_tracking_uri("databricks")
    # ❌ Missing: mlflow.set_registry_uri("databricks-uc")

    # Without these, MLflow defaults to local file/SQLite tracking
    experiment_name = config.databricks.experiment_name
    mlflow.set_experiment(experiment_name)  # Creates experiment locally
    ...
```

### Issue 2: Missing Unity Catalog three-level namespace

The `[deploy.databricks]` config includes `catalog` and `schema` fields, but they are never used. The model is registered with a flat name (`haute-test`) instead of the Unity Catalog three-level namespace (`workspace.default.haute-test`):

```python
# Current code — flat name
mlflow.pyfunc.log_model(
    ...
    registered_model_name=model_name,  # "haute-test"
)

# Should be — Unity Catalog three-level namespace
uc_model_name = f"{config.databricks.catalog}.{config.databricks.schema}.{model_name}"
mlflow.pyfunc.log_model(
    ...
    registered_model_name=uc_model_name,  # "workspace.default.haute-test"
)
```

### Issue 3: `get_deploy_status()` references undefined `uc_model_name`

The `get_deploy_status()` function in `_mlflow.py` references `uc_model_name`, which is a local variable from `deploy_to_mlflow()` and is not in scope. Calling this function will crash with a `NameError`:

```python
def get_deploy_status(model_name: str) -> dict[str, str | int]:
    import mlflow

    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{uc_model_name}'")  # ❌ NameError
    ...
```

The function accepts `model_name` as a parameter but never uses it — it references the nonexistent `uc_model_name` instead. It also needs to set the tracking/registry URIs and construct the UC name from the config, or accept the full UC model name directly.

### Issue 4: Experiment parent directories not auto-created

When the experiment path is `/Shared/haute/haute-test` and the `/Shared/haute` directory doesn't exist in the Databricks workspace, `mlflow.set_experiment()` fails with:

```
RESOURCE_DOES_NOT_EXIST: Parent directory /Shared/haute does not exist.
```

Databricks does not auto-create parent directories for experiments. The deploy should ensure they exist before calling `mlflow.set_experiment()`.

## Suggested Fix

### Fix 1 & 2: Set tracking URI, registry URI, and use UC model name

In `haute/deploy/_mlflow.py`, add the following at the top of `deploy_to_mlflow()`:

```diff
  def deploy_to_mlflow(resolved: ResolvedDeploy) -> DeployResult:
      import mlflow

      config = resolved.config
      model_name = config.model_name

+     # Point MLflow at the Databricks workspace (uses DATABRICKS_HOST/TOKEN env vars)
+     mlflow.set_tracking_uri("databricks")
+     mlflow.set_registry_uri("databricks-uc")
+
+     # Use Unity Catalog three-level namespace: catalog.schema.model_name
+     uc_model_name = f"{config.databricks.catalog}.{config.databricks.schema}.{model_name}"

      # 1. Build deployment manifest
      manifest = _build_manifest(resolved)
```

Then replace all downstream uses of `model_name` in MLflow registration calls with `uc_model_name`:

```diff
              mlflow.pyfunc.log_model(
                  artifact_path="model",
                  python_model=_get_model_instance(),
                  artifacts=artifacts,
                  signature=signature,
                  pip_requirements=_pip_requirements(resolved),
-                 registered_model_name=model_name,
+                 registered_model_name=uc_model_name,
              )

          client = mlflow.tracking.MlflowClient()
-         versions = client.search_model_versions(f"name='{model_name}'")
+         versions = client.search_model_versions(f"name='{uc_model_name}'")
          ...

-         model_uri = f"models:/{model_name}/{latest_version}"
+         model_uri = f"models:/{uc_model_name}/{latest_version}"
```

### Fix 3: Fix `get_deploy_status()` to use the correct model name and URIs

```diff
- def get_deploy_status(model_name: str) -> dict[str, str | int]:
+ def get_deploy_status(model_name: str, catalog: str = "main", schema: str = "default") -> dict[str, str | int]:
      import mlflow

+     mlflow.set_tracking_uri("databricks")
+     mlflow.set_registry_uri("databricks-uc")
+
+     uc_model_name = f"{catalog}.{schema}.{model_name}"
      client = mlflow.tracking.MlflowClient()
-     versions = client.search_model_versions(f"name='{uc_model_name}'")
+     versions = client.search_model_versions(f"name='{uc_model_name}'")
      ...
```

Alternatively, the function could accept the full UC model name directly and skip the construction.

### Fix 4: Auto-create experiment parent directories

Add a helper function that ensures the experiment's parent directory exists before `mlflow.set_experiment()` is called. This uses the Databricks Workspace API's `mkdirs` endpoint, which behaves like `mkdir -p` (creates all missing parent directories, no-ops if they already exist):

```python
def _ensure_experiment_directory(experiment_name: str) -> None:
    """Create parent directories for the experiment path in the Databricks workspace.

    Uses the Databricks Workspace REST API 'mkdirs' endpoint, which is
    idempotent — it creates all missing ancestors and no-ops if the
    directory already exists.

    Requires DATABRICKS_HOST and DATABRICKS_TOKEN in the environment
    (already loaded from .env by _load_env()).
    """
    import os
    from pathlib import PurePosixPath

    import requests

    parent_dir = str(PurePosixPath(experiment_name).parent)
    if parent_dir in ("/", "."):
        return  # Top-level path, nothing to create

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        return  # Cannot create directories without credentials; let MLflow fail naturally

    resp = requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": parent_dir},
    )
    resp.raise_for_status()
```

Then call it in `deploy_to_mlflow()` just before `mlflow.set_experiment()`:

```diff
          # 5. Set experiment if configured
          experiment_name = config.databricks.experiment_name
+         _ensure_experiment_directory(experiment_name)
          mlflow.set_experiment(experiment_name)
```

### Verification

After applying all three fixes:

```
$ haute deploy
  ...
  ✓ Test quotes: batch_policies.json              5 rows  ok  (7.7ms)
  ✓ Test quotes: edge_cases.json                  3 rows  ok  (6.7ms)
  ✓ Test quotes: single_policy.json               1 rows  ok  (6.9ms)
Successfully registered model 'workspace.default.haute-test'.
Created version '1' of model 'workspace.default.haute-test'.
🏃 View run deploy-haute-test at: https://dbc-xxxxx.cloud.databricks.com/ml/experiments/.../runs/...
🧪 View experiment at: https://dbc-xxxxx.cloud.databricks.com/ml/experiments/...
  ✓ Logged MLflow model: haute-test v1
  ✓ Model URI: models:/workspace.default.haute-test/1
```

The model is now visible in the Databricks MLflow UI under the Unity Catalog at `workspace.default.haute-test`.

## Impact

- **Severity**: High — `haute deploy` with `target = "databricks"` silently deploys to a local MLflow store instead of the configured Databricks workspace. Users believe the deploy succeeded but the model is not accessible remotely.
- **Affected versions**: haute 0.1.10 (and likely all prior versions with Databricks deploy support).
- **Workaround**: Manually patch `_mlflow.py` in the installed package as described above. The patch is lost on reinstall.
