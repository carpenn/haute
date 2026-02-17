"""Pipeline: my_pipeline"""

import polars as pl
import haute

import numpy as np
from catboost import CatBoostClassifier, CatBoostRegressor

pipeline = haute.Pipeline("my_pipeline", description="")


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


@pipeline.node(path="data/policies.parquet", deploy_input=True)
def policies() -> pl.LazyFrame:
    """data_source node"""
    return pl.scan_parquet("data/policies.parquet")


@pipeline.node(external="models/freq.cbm", file_type="catboost", model_class="regressor")
def frequency_model(policies: pl.LazyFrame) -> pl.LazyFrame:
    """catboost_load node"""
    from catboost import CatBoostRegressor
    obj = CatBoostRegressor()
    obj.load_model("models/freq.cbm")
    X = (
        policies
        .select(obj.feature_names_)
    ).collect().to_numpy()
    
    preds = obj.predict(X)
    
    df = (
        policies
        .select('IDpol')
        .with_columns(freq_preds = pl.Series(preds))
    )
    return df


@pipeline.node
def frequency_set(policies: pl.LazyFrame, exposure: pl.LazyFrame, claims_aggregate: pl.LazyFrame) -> pl.LazyFrame:
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
    from catboost import CatBoostRegressor
    obj = CatBoostRegressor()
    obj.load_model("models/sev.cbm")
    X = (
        policies
        .select(obj.feature_names_)
    ).collect().to_numpy()
    
    preds = obj.predict(X)
    
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
pipeline.connect("policies", "frequency_set")
pipeline.connect("exposure", "frequency_set")
pipeline.connect("exposure", "severity_set")
pipeline.connect("claims", "severity_set")
pipeline.connect("policies", "severity_set")
pipeline.connect("claims", "claims_aggregate")
pipeline.connect("claims_aggregate", "frequency_set")
pipeline.connect("frequency_set", "frequency_write")
pipeline.connect("severity_set", "severity_write")
pipeline.connect("policies", "frequency_model")
pipeline.connect("policies", "severity_model")
pipeline.connect("severity_model", "calculate_premium")
pipeline.connect("frequency_model", "calculate_premium")
pipeline.connect("calculate_premium", "output")
