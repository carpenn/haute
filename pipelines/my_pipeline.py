"""Pipeline: my_pipeline"""

import polars as pl
import runw

pipeline = runw.Pipeline("my_pipeline", description="")


@pipeline.node(path="pipelines/data/claims_amounts.parquet")
def claims() -> pl.DataFrame:
    """claims node"""
    return pl.scan_parquet("pipelines/data/claims_amounts.parquet")


@pipeline.node
def claims_aggregate(claims: pl.DataFrame) -> pl.DataFrame:
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


@pipeline.node(path="pipelines/data/exposure.parquet")
def exposure() -> pl.DataFrame:
    """exposure node"""
    return pl.scan_parquet("pipelines/data/exposure.parquet")


@pipeline.node(path="pipelines/data/policies.parquet")
def policies() -> pl.DataFrame:
    """data_source node"""
    return pl.scan_parquet("pipelines/data/policies.parquet")


@pipeline.node
def severity_set(exposure: pl.DataFrame, claims: pl.DataFrame, policies: pl.DataFrame) -> pl.DataFrame:
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


@pipeline.node
def frequency_set(policies: pl.DataFrame, exposure: pl.DataFrame, claims_aggregate: pl.DataFrame) -> pl.DataFrame:
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



# Wire nodes together — edges define data flow
pipeline.connect("policies", "frequency_set")
pipeline.connect("exposure", "frequency_set")
pipeline.connect("exposure", "severity_set")
pipeline.connect("claims", "severity_set")
pipeline.connect("policies", "severity_set")
pipeline.connect("claims", "claims_aggregate")
pipeline.connect("claims_aggregate", "frequency_set")
