# Bug: Migrate model logging from CloudPickle to models-from-code

## Summary

`haute deploy` currently passes a `HauteModel()` Python object to `mlflow.pyfunc.log_model(python_model=...)`, which causes MLflow to serialize it with CloudPickle. This creates two problems:

1. **CloudPickle serializes by reference** — on Databricks, the model is reconstructed by importing from the *installed* `haute` package, so any version mismatch between the deploying machine and the serving environment causes failures.
2. **MLflow 3.x deprecation** — passing a Python object to `python_model` emits a `FutureWarning` recommending the models-from-code approach instead.

The fix is to migrate from CloudPickle to MLflow's **models-from-code** pattern: pass a file path to a `.py` script that calls `mlflow.models.set_model()`. The script is saved as a plain artifact alongside the model — no serialization, no class reconstruction, no version coupling.

## Background: How CloudPickle causes the problem

When `_get_model_instance()` returns a `HauteModel()` object and it's passed to `mlflow.pyfunc.log_model()`:

1. CloudPickle sees `HauteModel` is defined at module scope in an installed package (`haute.deploy._model`)
2. It serializes the class **by reference** — stores only the import path `haute.deploy._model.HauteModel`
3. On Databricks, the serving environment installs `haute` from PyPI via `pip_requirements`
4. CloudPickle reconstructs the class by importing from the installed package
5. If the installed version differs from what was used to deploy, the class definition is wrong

This is fragile and unnecessary. The models-from-code approach ships the model definition as a plain `.py` file — no pickle, no import resolution, no version coupling.

## Exact changes required

Three changes across two files:

### Change 1: Create `haute/deploy/_model_code.py` (new file)

This is the models-from-code entrypoint. MLflow saves this script as a model artifact and executes it at load time to instantiate the model.

```python
"""MLflow models-from-code entrypoint for HauteModel."""

import json
from pathlib import Path

import mlflow.pyfunc
from mlflow.models import set_model


class HauteModel(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel wrapper for a deployed haute pipeline."""

    def load_context(self, context):
        """Called once when the model is loaded for serving."""
        manifest_path = Path(context.artifacts["deploy_manifest"])
        self._manifest = json.loads(manifest_path.read_text())
        self._graph = self._manifest["graph"]
        self._input_node_ids = self._manifest["input_nodes"]
        self._output_node_id = self._manifest["output_node"]
        self._output_fields = self._manifest.get("output_fields")

        self._artifact_paths = {}
        for artifact_name in self._manifest.get("artifacts", {}):
            if artifact_name in context.artifacts:
                self._artifact_paths[artifact_name] = context.artifacts[artifact_name]

    def predict(self, context, model_input, params=None):
        """Score one or more rows through the pipeline."""
        import polars as pl
        from haute.deploy._scorer import score_graph

        input_df = pl.from_pandas(model_input)
        result = score_graph(
            graph=self._graph,
            input_df=input_df,
            input_node_ids=self._input_node_ids,
            output_node_id=self._output_node_id,
            artifact_paths=self._artifact_paths,
            output_fields=self._output_fields,
        )
        return result.to_pandas()


set_model(HauteModel())
```

Note: This file duplicates the class from `_model.py`. The `_model.py` file can be kept for backwards compatibility or removed — the models-from-code script is self-contained.

### Change 2: Update `log_model()` call in `haute/deploy/_mlflow.py`

In `deploy_to_mlflow()`, replace the CloudPickle-based logging with the file path approach:

```diff
+ import importlib.resources
+
+ # Resolve the path to the models-from-code script shipped with the package
+ _MODEL_CODE_PATH = str(importlib.resources.files("haute.deploy") / "_model_code.py")

  # Inside deploy_to_mlflow(), the log_model call:
  mlflow.pyfunc.log_model(
-     artifact_path="model",
-     python_model=_get_model_instance(),
+     name="model",
+     python_model=_MODEL_CODE_PATH,
      artifacts=artifacts,
      signature=signature,
      pip_requirements=_pip_requirements(resolved),
      registered_model_name=uc_model_name,
  )
```

Key changes:
- `python_model` receives a **file path** (str) instead of a Python object
- `artifact_path` is replaced with `name` (the former is deprecated in MLflow 3.x)

### Change 3: Remove `_get_model_instance()` from `haute/deploy/_mlflow.py`

The function is no longer needed and can be deleted entirely:

```diff
- def _get_model_instance() -> object:
-     """Import and return a HauteModel instance.
-
-     Deferred import to avoid loading mlflow at module level.
-     """
-     from haute.deploy._model import HauteModel
-     return HauteModel()
```

## What this resolves

- **No more CloudPickle** — the model script is a plain `.py` file in the MLflow artifact store
- **No version coupling** — the class definition travels with the model, not reconstructed from the installed package
- **No `FutureWarning`** — MLflow no longer warns about CloudPickle serialization
- **No `artifact_path` deprecation warning** — uses the new `name` parameter
- **Deterministic behaviour** — the exact code that was deployed is the exact code that runs on Databricks

## Verification

After applying these changes:

```
$ haute deploy
  ...
  ✓ Logged MLflow model: haute-test v1
  ✓ Model URI: models:/workspace.default.haute-test/1

Endpoint ready:
  POST https://dbc-xxxxx.cloud.databricks.com/serving-endpoints/haute-test/invocations
```

Then test the endpoint:

```bash
curl -X POST "$DATABRICKS_HOST/serving-endpoints/haute-test/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"dataframe_records": [{"IDpol": 99001, "VehPower": 7, "VehAge": 3, ...}]}'
```

Should return predictions without the `predict_type_hints` error.

## Impact

- **Severity**: Critical — all Databricks Model Serving deployments via `haute deploy` fail at inference time with `AttributeError: 'HauteModel' object has no attribute 'predict_type_hints'`
- **Affected versions**: haute 0.1.10 (and likely all versions with Databricks deploy support)
- **User experience**: `haute deploy` reports success, the endpoint shows "Ready" in Databricks, but every inference request returns a 400 error
