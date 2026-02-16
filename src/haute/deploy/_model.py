"""MLflow PythonModel wrapper for deployed haute pipelines.

``HauteModel`` is logged as an MLflow pyfunc model. At serving time:

1. ``load_context()`` reads the deployment manifest and resolves artifact paths.
2. ``predict()`` converts pandas → Polars, scores through the pruned graph,
   and converts back to pandas.

**Documented exception to "Polars-native, never convert to pandas":**
MLflow's PythonModel.predict() contract requires pandas DataFrames.
The conversion happens at the outermost boundary only — all internal
computation remains Polars LazyFrame throughout.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import mlflow.pyfunc

if TYPE_CHECKING:
    import pandas as pd
    from mlflow.pyfunc import PythonModelContext


class HauteModel(mlflow.pyfunc.PythonModel):
    """MLflow PythonModel wrapper for a deployed haute pipeline.

    This class is instantiated by MLflow when the model is loaded for
    serving.  It is not instantiated directly.

    Artifacts:
        - ``deploy_manifest.json``: graph, config, schemas, artifact map
        - Model files: ``.cbm``, ``.pkl``, ``.joblib``, etc.
        - Static data: ``.parquet``, ``.csv`` for non-input data sources
    """

    def load_context(self, context: PythonModelContext) -> None:
        """Called once when the model is loaded for serving.

        Loads the deployment manifest and resolves artifact paths to
        the MLflow artifact directory.
        """
        manifest_path = Path(context.artifacts["deploy_manifest"])
        self._manifest: dict = json.loads(manifest_path.read_text())
        self._graph: dict = self._manifest["graph"]
        self._input_node_ids: list[str] = self._manifest["input_nodes"]
        self._output_node_id: str = self._manifest["output_node"]
        self._output_fields: list[str] | None = self._manifest.get("output_fields")

        # Remap artifact paths to MLflow artifact directory
        self._artifact_paths: dict[str, str] = {}
        for artifact_name in self._manifest.get("artifacts", {}):
            if artifact_name in context.artifacts:
                self._artifact_paths[artifact_name] = context.artifacts[artifact_name]

    def predict(
        self,
        context: PythonModelContext,
        model_input: pd.DataFrame,
        params: dict | None = None,
    ) -> pd.DataFrame:
        """Score one or more rows through the pipeline.

        Args:
            context: MLflow model context (unused after load_context).
            model_input: Input pandas DataFrame (MLflow convention).
            params: Optional prediction parameters (unused).

        Returns:
            Output pandas DataFrame.
        """
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
