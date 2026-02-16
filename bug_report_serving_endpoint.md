# Bug: `haute deploy` never creates a Databricks Model Serving endpoint despite `endpoint_name` being configured

## Summary

When `[deploy] endpoint_name` is set in `haute.toml`, `haute deploy` registers the model in MLflow but never creates or updates a Databricks Model Serving endpoint. The `deploy_to_mlflow()` function in `haute/deploy/_mlflow.py` hardcodes `endpoint_url=None` in its return value. The serving-related config fields (`endpoint_name`, `serving_workload_size`, `serving_scale_to_zero`) are parsed from TOML and stored in the `DeployConfig` / `DatabricksConfig` dataclasses but never consumed.

The CLI even has downstream code that conditionally displays the endpoint URL when it's present — but it's never set, so the user always sees the "Serve locally" fallback message instead.

## Environment

| Component | Version |
|-----------|---------|
| **haute** | 0.1.10 |
| **mlflow** | 3.9.0 |
| **databricks-sdk** | 0.88.0 |
| **Python** | 3.13.7 |
| **OS** | Ubuntu Linux |

## Steps to Reproduce

1. Install `haute==0.1.10` with the Databricks SDK available.
2. Configure `haute.toml` with an endpoint name and serving settings:
   ```toml
   [deploy]
   target = "databricks"
   model_name = "haute-test"
   endpoint_name = "haute-test"

   [deploy.databricks]
   experiment_name = "/Shared/haute/haute-test"
   catalog = "workspace"
   schema = "default"
   serving_workload_size = "Small"
   serving_scale_to_zero = true
   ```
3. Run `haute deploy`.

### Expected behavior

After registering the model in MLflow, haute creates (or updates) a Databricks Model Serving endpoint named `haute-test` serving the newly registered model version, and outputs:

```
Endpoint ready:
  POST https://dbc-xxxxx.cloud.databricks.com/serving-endpoints/haute-test/invocations
```

### Actual behavior

The model is registered successfully, but no serving endpoint is created. The output falls through to the local-serve fallback:

```
Deploy complete. Serve locally with:
  mlflow models serve -m "models:/workspace.default.haute-test/1" -p 5001
```

## Root Cause

### Unused config fields

`DatabricksConfig` in `haute/deploy/_config.py` correctly parses all serving-related fields:

```python
@dataclass
class DatabricksConfig:
    experiment_name: str = "/Shared/haute/default"
    catalog: str = "main"
    schema: str = "pricing"
    serving_workload_size: str = "Small"       # ← parsed but never used
    serving_scale_to_zero: bool = True         # ← parsed but never used
```

And `DeployConfig` stores `endpoint_name`:

```python
@dataclass
class DeployConfig:
    pipeline_file: Path
    model_name: str
    endpoint_name: str | None = None           # ← parsed but never used
    ...
```

### Missing endpoint creation step

In `haute/deploy/_mlflow.py`, `deploy_to_mlflow()` stops after model registration and returns `endpoint_url=None`:

```python
def deploy_to_mlflow(resolved: ResolvedDeploy) -> DeployResult:
    ...
    # Steps 1–7: build manifest, log model, register in MLflow
    ...

    return DeployResult(
        model_name=model_name,
        model_version=int(latest_version),
        model_uri=model_uri,
        endpoint_url=None,          # ← hardcoded, never creates an endpoint
        manifest_path=manifest_path,
    )
```

### Dead CLI code

The CLI in `haute/cli.py` has code to display the endpoint URL, but it's unreachable because `endpoint_url` is always `None`:

```python
# cli.py lines 464–468
if result.endpoint_url:
    click.echo(f"\nEndpoint ready:\n  POST {result.endpoint_url}")
else:
    click.echo("\nDeploy complete. Serve locally with:")
    click.echo(f'  mlflow models serve -m "{result.model_uri}" -p 5001')
```

## Suggested Fix

Add a `_create_or_update_serving_endpoint()` helper function to `haute/deploy/_mlflow.py` that uses the Databricks SDK to create or update the serving endpoint, and wire it into `deploy_to_mlflow()`.

### New helper function

```python
def _create_or_update_serving_endpoint(
    config: DeployConfig,
    uc_model_name: str,
    model_version: int,
) -> str | None:
    """Create or update a Databricks Model Serving endpoint.

    Uses the Databricks SDK to create an endpoint if it doesn't exist,
    or update the served model version if it does.

    Args:
        config: The deployment configuration (provides endpoint_name and
                serving settings from haute.toml).
        uc_model_name: The Unity Catalog three-level model name
                       (e.g. "workspace.default.haute-test").
        model_version: The model version number to serve.

    Returns:
        The endpoint invocation URL, or None if endpoint_name is not configured.
    """
    import os

    endpoint_name = config.endpoint_name
    if not endpoint_name:
        return None

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.serving import (
        EndpointCoreConfigInput,
        ServedEntityInput,
    )

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    w = WorkspaceClient(host=host, token=token)

    served_entity = ServedEntityInput(
        entity_name=uc_model_name,
        entity_version=str(model_version),
        workload_size=config.databricks.serving_workload_size,
        scale_to_zero_enabled=config.databricks.serving_scale_to_zero,
    )

    endpoint_config = EndpointCoreConfigInput(
        name=endpoint_name,
        served_entities=[served_entity],
    )

    # Check if endpoint already exists
    try:
        existing = w.serving_endpoints.get(endpoint_name)
        # Endpoint exists — update the served model version
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[served_entity],
        )
    except Exception:
        # Endpoint doesn't exist — create it
        w.serving_endpoints.create(
            name=endpoint_name,
            config=endpoint_config,
        )

    endpoint_url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
    return endpoint_url
```

### Wire into `deploy_to_mlflow()`

```diff
      model_uri = f"models:/{uc_model_name}/{latest_version}"

+ # 8. Create or update the serving endpoint
+ endpoint_url = _create_or_update_serving_endpoint(
+     config=config,
+     uc_model_name=uc_model_name,
+     model_version=int(latest_version),
+ )

  return DeployResult(
      model_name=model_name,
      model_version=int(latest_version),
      model_uri=model_uri,
-     endpoint_url=None,
+     endpoint_url=endpoint_url,
      manifest_path=manifest_path,
  )
```

### Verification

After applying the fix:

```
$ haute deploy
  ✓ Loaded config from haute.toml

Deploying pipeline: haute-test
  ...
  ✓ Test quotes: batch_policies.json              5 rows  ok  (5.6ms)
  ✓ Test quotes: edge_cases.json                  3 rows  ok  (5.0ms)
  ✓ Test quotes: single_policy.json               1 rows  ok  (5.4ms)
Registered model 'workspace.default.haute-test' already exists. Creating a new version of this model...
Created version '3' of model 'workspace.default.haute-test'.
  ✓ Logged MLflow model: haute-test v3
  ✓ Model URI: models:/workspace.default.haute-test/3

Endpoint ready:
  POST https://dbc-xxxxx.cloud.databricks.com/serving-endpoints/haute-test/invocations
```

The serving endpoint is now created in Databricks and begins provisioning. The CLI correctly displays the endpoint URL instead of the local-serve fallback.

## Additional Notes

- **Idempotent**: The fix uses a get-or-create pattern — if the endpoint already exists, it updates the served entity to the new model version. If not, it creates a new endpoint.
- **`databricks-sdk` dependency**: The fix uses `databricks.sdk.WorkspaceClient` and `databricks.sdk.service.serving` types. The SDK is already installed as a dependency of haute's Databricks deploy target. The import is deferred so it doesn't affect non-Databricks deployments.
- **Graceful fallback**: If `endpoint_name` is not set in `haute.toml`, the function returns `None` and the CLI falls through to the existing local-serve message — preserving backward compatibility.
- **Endpoint provisioning time**: Databricks Model Serving endpoints take several minutes to provision after creation. The endpoint URL is returned immediately, but the endpoint status will transition from "Pending" → "Ready" asynchronously.

## Impact

- **Severity**: High — the `endpoint_name`, `serving_workload_size`, and `serving_scale_to_zero` config fields are documented and parsed but silently ignored. Users expect `haute deploy` to create a live serving endpoint, but it only registers the model.
- **Affected versions**: haute 0.1.10 (and likely all prior versions with Databricks deploy support).
- **Workaround**: Manually create the serving endpoint in the Databricks UI or via the SDK after running `haute deploy`, or patch `_mlflow.py` in the installed package as described above.
