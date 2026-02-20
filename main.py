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


@pipeline.node(output=True)
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output 13 node"""
    return calculate_premium


@pipeline.node(api_input=True, path="data/IDpol_1052049.json", row_id_column="IDpol")
def quotes() -> pl.LazyFrame:
    """api_input node"""
    return pl.read_json("data/IDpol_1052049.json").lazy()


@pipeline.node(live_switch=True)
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return quotes


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


pipeline.submodel("modules/model_scoring.py")


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
