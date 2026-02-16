# Bug: `haute deploy` fails with MLflow 3.x — `HauteModel` does not inherit from `mlflow.pyfunc.PythonModel`

## Summary

`haute deploy` fails at the MLflow model logging step with the error:

```
python_model` must be a PythonModel instance, callable object, or path to a script
that uses set_model() to set a PythonModel instance or callable object.
```

This is because `haute.deploy._model.HauteModel` is a plain Python class and does not inherit from `mlflow.pyfunc.PythonModel`. MLflow 3.x enforces a strict type check on the `python_model` argument passed to `mlflow.pyfunc.log_model()`, which rejects any object that is not a `PythonModel` subclass, a callable, or a script path.

## Environment

| Component | Version |
|-----------|---------|
| **haute** | 0.1.10 |
| **mlflow** | 3.9.0 |
| **Python** | 3.13.7 |
| **OS** | Ubuntu Linux |

## Steps to Reproduce

1. Install `haute==0.1.10` with `mlflow>=3.0.0` (tested with 3.9.0).
2. Set up a valid haute project with a `haute.toml` targeting Databricks deployment:
   ```toml
   [deploy]
   target = "databricks"
   model_name = "my-model"
   endpoint_name = "my-model"

   [deploy.databricks]
   experiment_name = "/Shared/haute/my-model"
   ```
3. Run `haute deploy`.
4. Deployment completes validation and test quotes successfully, but fails at the MLflow model logging step.

## Full Error Output

```
$ haute deploy
  ✓ Loaded config from haute.toml

Deploying pipeline: haute-test
  Pipeline: main.py
  ✓ Parsed pipeline (5 nodes, 5 edges)
  ✓ Pruned to output ancestors (5 nodes)
  ✓ Collected 2 artifacts
  ✓ Input node(s): quotes
  ✓ Output node: output
  ✓ Inferred input schema (10 columns)
  ✓ Inferred output schema (5 columns)
  ✓ Validation passed
  ✓ Test quotes: batch_policies.json              5 rows  ok  (7.6ms)
  ✓ Test quotes: edge_cases.json                  3 rows  ok  (7.0ms)
  ✓ Test quotes: single_policy.json               1 rows  ok  (8.6ms)
  [... alembic migration output omitted for brevity ...]

  ✗ Deployment failed: `python_model` must be a PythonModel instance, callable object,
    or path to a script that uses set_model() to set a PythonModel instance or callable object.
```

## Root Cause

In `haute/deploy/_model.py`, the `HauteModel` class is defined as a plain class:

```python
class HauteModel:
    """MLflow PythonModel wrapper for a deployed haute pipeline."""

    def load_context(self, context: PythonModelContext) -> None:
        ...

    def predict(self, context: PythonModelContext, model_input, params=None):
        ...
```

In `haute/deploy/_mlflow.py`, this class is instantiated and passed to `mlflow.pyfunc.log_model()`:

```python
def _get_model_instance() -> object:
    from haute.deploy._model import HauteModel
    return HauteModel()

# Later, in deploy_to_mlflow():
mlflow.pyfunc.log_model(
    artifact_path="model",
    python_model=_get_model_instance(),  # <-- fails type check
    artifacts=artifacts,
    signature=signature,
    pip_requirements=_pip_requirements(resolved),
    registered_model_name=model_name,
)
```

MLflow 2.x was lenient and accepted duck-typed objects with `load_context()` and `predict()` methods. **MLflow 3.x introduced a strict `isinstance()` check** that requires `python_model` to be one of:

- An instance of `mlflow.pyfunc.PythonModel`
- A callable object
- A file path string to a script using `set_model()`

Since `HauteModel` doesn't inherit from `PythonModel`, the check fails.

## Suggested Fix

In `haute/deploy/_model.py`, change `HauteModel` to inherit from `mlflow.pyfunc.PythonModel`:

```diff
- from typing import TYPE_CHECKING
+ from typing import TYPE_CHECKING
+ 
+ import mlflow.pyfunc

  if TYPE_CHECKING:
      import pandas as pd
      from mlflow.pyfunc import PythonModelContext


- class HauteModel:
+ class HauteModel(mlflow.pyfunc.PythonModel):
```

This is a one-line inheritance change plus one import. No other modifications are needed — `HauteModel` already implements the correct `load_context()` and `predict()` method signatures that `PythonModel` expects.

### Verification

After applying this fix locally, `haute deploy` completes successfully:

```
  ✓ Logged MLflow model: haute-test v1
  ✓ Model URI: models:/haute-test/1

Deploy complete. Serve locally with:
  mlflow models serve -m "models:/haute-test/1" -p 5001
```

## Additional Notes

- The `_get_model_instance()` function in `_mlflow.py` has a return type annotation of `object`, which further suggests the original intent was duck-typing. The return type could also be tightened to `mlflow.pyfunc.PythonModel` for clarity.
- The `artifact_path` parameter in the `log_model()` call also triggers a deprecation warning in MLflow 3.x (`artifact_path` is deprecated in favor of `name`). This is a separate issue but worth addressing in the same pass.
- This bug blocks **all** Databricks deployments when using `mlflow>=3.0.0`.
