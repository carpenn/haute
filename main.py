"""Pipeline: my_pipeline"""

import polars as pl
import haute

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.node(path="data/claims_amounts.parquet")
def claims() -> pl.LazyFrame:
    """claims node"""
    return pl.scan_parquet("data/claims_amounts.parquet")


@pipeline.node(path="data/exposure.parquet")
def exposure() -> pl.LazyFrame:
    """exposure node"""
    return pl.scan_parquet("data/exposure.parquet")


@pipeline.node(sink="output/frequency.parquet", format="parquet")
def frequency_write(frequency_set: pl.LazyFrame) -> pl.LazyFrame:
    """frequency_write node"""
    frequency_set.collect().write_parquet("output/frequency.parquet")
    return frequency_set


@pipeline.node(api_input=True, path="data/IDpol_1052049.json", row_id_column="IDpol")
def policies() -> pl.LazyFrame:
    """api_input node"""
    return pl.read_json("data/IDpol_1052049.json").lazy()


@pipeline.node(external="models/freq.cbm", file_type="catboost", model_class="regressor")
def frequency_model(policies: pl.LazyFrame) -> pl.LazyFrame:
    """catboost_load node"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("models/freq.cbm", "catboost", "regressor")
    features = (  # noqa: N806
        policies
        .select(obj.feature_names_)
    ).collect().to_numpy()
    
    preds = obj.predict(features)
    
    df = (
        policies
        .select('IDpol')
        .with_columns(freq_preds = pl.Series(preds))
    )
    return df


@pipeline.node(external="models/sev.cbm", file_type="catboost", model_class="regressor")
def severity_model(policies: pl.LazyFrame) -> pl.LazyFrame:
    """catboost_load node"""
    from haute.graph_utils import load_external_object
    obj = load_external_object("models/sev.cbm", "catboost", "regressor")
    features = (  # noqa: N806
        policies
        .select(obj.feature_names_)
    ).collect().to_numpy()
    
    preds = obj.predict(features)
    
    df = (
        policies
        .select('IDpol')
        .with_columns(sev_preds = pl.Series(preds))
    )
    return df


@pipeline.node
def calculate_premium(severity_model: pl.LazyFrame, frequency_model: pl.LazyFrame) -> pl.LazyFrame:
    """calculate_premium node"""
    df = (
    frequency_model
    .join(
        severity_model, 
        on = 'IDpol', 
        how = 'left'
    )
    .with_columns(technical_price = pl.col('freq_preds') * pl.col('sev_preds'))
    .with_columns(premium = pl.col('technical_price') / 0.7)
    )
    return df


@pipeline.node(output=True)
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output 13 node"""
    return calculate_premium


@pipeline.node(sink="output/severity.parquet", format="parquet")
def severity_write(severity_set: pl.LazyFrame) -> pl.LazyFrame:
    """severity_write node"""
    severity_set.collect().write_parquet("output/severity.parquet")
    return severity_set


pipeline.submodel("modules/model_scoring.py")


# Wire nodes together - edges define data flow
pipeline.connect("policies", "frequency_model")
pipeline.connect("policies", "severity_model")
pipeline.connect("severity_model", "calculate_premium")
pipeline.connect("frequency_model", "calculate_premium")
pipeline.connect("calculate_premium", "output")
pipeline.connect("policies", "frequency_set")
pipeline.connect("exposure", "frequency_set")
pipeline.connect("exposure", "severity_set")
pipeline.connect("claims", "severity_set")
pipeline.connect("policies", "severity_set")
pipeline.connect("claims", "claims_aggregate")
pipeline.connect("frequency_set", "frequency_write")
pipeline.connect("severity_set", "severity_write")
