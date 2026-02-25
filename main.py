"""Pipeline: my_pipeline"""

import polars as pl
import haute

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.node(config="config/datasource/batch_quotes.json")
def batch_quotes() -> pl.LazyFrame:
    """batch_quotes node"""
    from haute._databricks_io import read_cached_table
    return read_cached_table("quotes.delta.policies")


@pipeline.node(config="config/datasource/claims.json")
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


@pipeline.node(config="config/datasource/exposure.json")
def exposure() -> pl.LazyFrame:
    """exposure node"""
    return pl.scan_parquet("data/exposure.parquet")


@pipeline.node(config="config/api_input/quotes.json")
def quotes() -> pl.LazyFrame:
    """api_input node"""
    return pl.read_json("data/IDpol_1052049.json").lazy()


@pipeline.node(config="config/live_switch/policies.json")
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return batch_quotes


@pipeline.node(config="config/model_score/Model_Score_19.json")
def Model_Score_19(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Model Score 19 node"""
    df = policies
    from haute.graph_utils import load_mlflow_model
    model = load_mlflow_model(source_type="run", run_id="d5aa4ee011594c52bed9fff777376619", artifact_path="Model_Training_17.cbm", task="regression")
    # CatBoost requires numpy arrays; collect → predict → lazy is the minimum conversion
    df_eager = df.collect()
    features = [f for f in model.feature_names_ if f in df_eager.columns]
    X = df_eager.select(features).to_pandas()
    preds = model.predict(X).flatten()
    df_eager = df_eager.with_columns(pl.Series("prediction", preds))
    result = df_eager.lazy()
    return result


@pipeline.node(config="config/external_model/frequency_model.json")
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


@pipeline.node(config="config/sink/frequency_write.json")
def frequency_write(frequency_set: pl.LazyFrame) -> pl.LazyFrame:
    """frequency_write node"""
    frequency_set.collect().write_parquet("output/frequency.parquet")
    return frequency_set


@pipeline.node(config="config/factors/optimiser_banding.json")
def optimiser_banding(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Banding 15 node"""
    return df


@pipeline.node(config="config/tables/Rating_Step_16.json")
def Rating_Step_16(optimiser_banding: pl.LazyFrame) -> pl.LazyFrame:
    """Rating Step 16 node"""
    return df


@pipeline.node(config="config/optimiser_apply/apply_ratebook.json")
def apply_ratebook(optimiser_banding: pl.LazyFrame) -> pl.LazyFrame:
    """apply_ratebook node"""
    return optimiser_banding


@pipeline.node(config="config/external_model/severity_model.json")
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


@pipeline.node(config="config/output/output.json")
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output 13 node"""
    return calculate_premium


@pipeline.node(config="config/scenario_expander/price_scenarios.json")
def price_scenarios(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """price_scenarios node"""
    return calculate_premium


@pipeline.node
def optimiser_inputs(price_scenarios: pl.LazyFrame) -> pl.LazyFrame:
    """optimiser_inputs node"""
    df = (
    price_scenarios
    .with_columns(
        price_scenario = pl.col('price_multiplier') * pl.col('premium'),
        volume = (30-pl.col('scenario_index'))/100
    )
    .with_columns(
        income = (pl.col('price_scenario') - pl.col('technical_price') - 5) * pl.col('volume')
    )
    )
    return df


@pipeline.node(config="config/optimiser_apply/apply_online.json")
def apply_online(optimiser_inputs: pl.LazyFrame) -> pl.LazyFrame:
    """Apply Optimisation 23 node"""
    return optimiser_inputs


@pipeline.node(config="config/optimiser/online_optimisation.json")
def online_optimisation(optimiser_inputs: pl.LazyFrame) -> pl.LazyFrame:
    """online_optimisation node"""
    return optimiser_inputs


@pipeline.node(config="config/optimiser/ratebook_optimisation.json")
def ratebook_optimisation(optimiser_inputs: pl.LazyFrame, optimiser_banding: pl.LazyFrame) -> pl.LazyFrame:
    """Optimiser 24 node"""
    return optimiser_inputs


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


@pipeline.node(config="config/modelling/Model_Training_17.json")
def Model_Training_17(severity_set: pl.LazyFrame) -> pl.LazyFrame:
    """Model Training 17 node"""
    return df


@pipeline.node(config="config/sink/severity_write.json")
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
pipeline.connect("policies", "optimiser_banding")
pipeline.connect("optimiser_banding", "Rating_Step_16")
pipeline.connect("severity_set", "Model_Training_17")
pipeline.connect("policies", "Model_Score_19")
pipeline.connect("calculate_premium", "price_scenarios")
pipeline.connect("price_scenarios", "optimiser_inputs")
pipeline.connect("optimiser_inputs", "online_optimisation")
pipeline.connect("optimiser_inputs", "ratebook_optimisation")
pipeline.connect("optimiser_banding", "ratebook_optimisation")
pipeline.connect("optimiser_inputs", "apply_online")
pipeline.connect("optimiser_banding", "apply_ratebook")
