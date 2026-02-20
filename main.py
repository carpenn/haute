"""Pipeline: my_pipeline"""

import polars as pl
import haute

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.node(path="data/policies.parquet")
def batch_quotes() -> pl.LazyFrame:
    """batch_quotes node"""
    return pl.scan_parquet("data/policies.parquet")


@pipeline.node(path="data/claims_amounts.parquet")
def claims() -> pl.LazyFrame:
    """claims node"""
    return pl.scan_parquet("data/claims_amounts.parquet")


@pipeline.node
def claims_aggregate(claims: pl.LazyFrame) -> pl.LazyFrame:
    """claims_aggregate node"""
    df = (
    claims
    .with_columns(
        ClaimCount = 
            pl.when(pl.col('ClaimAmount') > 0)
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
    )
    .group_by('IDpol')
    .agg(
        pl.sum('ClaimCount')
    )
    )
    return df


@pipeline.node(path="data/exposure.parquet")
def exposure() -> pl.LazyFrame:
    """exposure node"""
    return pl.scan_parquet("data/exposure.parquet")


@pipeline.node(api_input=True, path="data/IDpol_1052049.json", row_id_column="IDpol")
def quotes() -> pl.LazyFrame:
    """api_input node"""
    return pl.read_json("data/IDpol_1052049.json").lazy()


@pipeline.node(live_switch=True)
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return quotes


@pipeline.node(factors=[{'banding': 'continuous', 'column': 'DrivAge', 'output_column': 'DrivAgeBand', 'rules': [{'op1': '>', 'val1': '0', 'op2': '', 'val2': '', 'assignment': 'positive'}]}, {'banding': 'continuous', 'column': 'DrivAge', 'output_column': 'DrivAgeBand', 'rules': [{'op1': '>', 'val1': '30', 'op2': '', 'val2': '', 'assignment': '30+'}]}])
def Banding_15(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Banding 15 node"""
    return df


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


@pipeline.node
def frequency_set(exposure: pl.LazyFrame, claims_aggregate: pl.LazyFrame, policies: pl.LazyFrame) -> pl.LazyFrame:
    """frequency_set node"""
    df = (
    policies
    .join(
        claims_aggregate, 
        on = 'IDpol', 
        how = 'left'
    )
    .join(
        exposure, 
        on = 'IDpol', 
        how = 'left'
    )
    )
    return df


@pipeline.node(sink="output/frequency.parquet", format="parquet")
def frequency_write(frequency_set: pl.LazyFrame) -> pl.LazyFrame:
    """frequency_write node"""
    frequency_set.collect().write_parquet("output/frequency.parquet")
    return frequency_set


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


@pipeline.node
def severity_set(exposure: pl.LazyFrame, claims: pl.LazyFrame, policies: pl.LazyFrame) -> pl.LazyFrame:
    """claim_exposure_join copy node"""
    df = (
    policies
    .join(
        claims, 
        on = 'IDpol', 
        how = 'left'
    )
    .filter(pl.col('ClaimAmount') > 0 )
    )
    return df


@pipeline.node(sink="output/severity.parquet", format="parquet")
def severity_write(severity_set: pl.LazyFrame) -> pl.LazyFrame:
    """severity_write node"""
    severity_set.collect().write_parquet("output/severity.parquet")
    return severity_set



# Wire nodes together - edges define data flow
pipeline.connect("exposure", "frequency_set")
pipeline.connect("exposure", "severity_set")
pipeline.connect("claims", "severity_set")
pipeline.connect("claims", "claims_aggregate")
pipeline.connect("frequency_set", "frequency_write")
pipeline.connect("severity_set", "severity_write")
pipeline.connect("quotes", "policies")
pipeline.connect("batch_quotes", "policies")
pipeline.connect("claims_aggregate", "frequency_set")
pipeline.connect("policies", "frequency_set")
pipeline.connect("policies", "severity_set")
pipeline.connect("calculate_premium", "output")
pipeline.connect("policies", "frequency_model")
pipeline.connect("policies", "severity_model")
pipeline.connect("severity_model", "calculate_premium")
pipeline.connect("frequency_model", "calculate_premium")
pipeline.connect("policies", "Banding_15")
