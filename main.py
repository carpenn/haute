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


@pipeline.node(live_switch=True, mode="batch_quotes")
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return batch_quotes


@pipeline.node(model_score=True, source_type="run", run_id="d5aa4ee011594c52bed9fff777376619", artifact_path="Model_Training_17.cbm", task="regression", output_column="prediction", run_name="Model_Training_17", experiment_name="/Shared/haute/Model_Training_17", experiment_id="2825776902945395")
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


@pipeline.node(factors=[{'banding': 'continuous', 'column': 'DrivAge', 'output_column': 'DrivAgeBand', 'rules': [{'op1': '>', 'val1': '0', 'op2': '<=', 'val2': '20', 'assignment': '0-20'}, {'op1': '>', 'val1': '20', 'op2': '<=', 'val2': '30', 'assignment': '20-30'}, {'op1': '>', 'val1': '30', 'op2': '<=', 'val2': '40', 'assignment': '30-40'}, {'op1': '>', 'val1': '40', 'op2': '<=', 'val2': '50', 'assignment': '50-60'}, {'op1': '>', 'val1': '50', 'op2': '', 'val2': '', 'assignment': '60+'}]}, {'banding': 'categorical', 'column': 'Region', 'output_column': 'RegionBand', 'rules': [{'value': 'Centre', 'assignment': 'Centre'}, {'value': 'London', 'assignment': 'London'}, {'value': 'Paris', 'assignment': 'Paris'}], 'default': 'Other'}, {'banding': 'continuous', 'column': 'VehPower', 'output_column': 'VehPowerBand', 'rules': [{'op1': '>=', 'val1': '0', 'op2': '<', 'val2': '3', 'assignment': '0-3'}, {'op1': '>=', 'val1': '3', 'op2': '<', 'val2': '6', 'assignment': '3-6'}, {'op1': '>=', 'val1': '6', 'op2': '', 'val2': '', 'assignment': '6+'}]}])
def optimiser_banding(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Banding 15 node"""
    return df


@pipeline.node(tables=[{'name': 'Multit', 'factors': ['DrivAgeBand', 'RegionBand', 'VehPowerBand'], 'output_column': 'Multi', 'entries': [{'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 2}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}], 'default_value': '1.0'}])
def Rating_Step_16(optimiser_banding: pl.LazyFrame) -> pl.LazyFrame:
    """Rating Step 16 node"""
    return df


@pipeline.node(optimiser_apply=True, version_column='__optimiser_version__', sourceType='run', experiment_id='1297322192316636', run_id='db866caae8d7451bba18285a2c669b71')
def apply_ratebook(optimiser_banding: pl.LazyFrame) -> pl.LazyFrame:
    """apply_ratebook node"""
    return optimiser_banding


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


@pipeline.node(scenario_expander=True, quote_id='IDpol', column_name='price_multiplier', min_value=0.8, max_value=1.2, steps=21, step_column='scenario_index')
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


@pipeline.node(optimiser_apply=True, version_column='__optimiser_version__', sourceType='run', experiment_id='1297322192316636', experiment_name='/optimisation', run_id='fe9fcd1fe3734556af235b4335837e70', run_name='online_optimisation')
def apply_online(optimiser_inputs: pl.LazyFrame) -> pl.LazyFrame:
    """Apply Optimisation 23 node"""
    return optimiser_inputs


@pipeline.node(optimiser=True, mode='online', quote_id='IDpol', scenario_index='scenario_index', scenario_value='price_multiplier', objective='income', constraints={'volume': {'min': 0.9}}, max_iter=50, tolerance=1e-06, data_input='optimiser_inputs')
def online_optimisation(optimiser_inputs: pl.LazyFrame) -> pl.LazyFrame:
    """online_optimisation node"""
    return optimiser_inputs


@pipeline.node(optimiser=True, mode='ratebook', quote_id='IDpol', scenario_index='scenario_index', scenario_value='price_multiplier', objective='income', constraints={'volume': {'min': 0.9}}, max_iter=50, tolerance=1e-06, factor_columns=[['DrivAgeBand'], ['RegionBand'], ['VehPowerBand']], data_input='optimiser_inputs', banding_source='optimiser_banding')
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


@pipeline.node(modelling=True, target='ClaimAmount', exclude=['IDpol'], algorithm='catboost', task='regression', params={'iterations': 1000, 'learning_rate': 0.05, 'depth': 6}, split={'strategy': 'random', 'test_size': 0.2, 'seed': 42}, metrics=['rmse'])
def Model_Training_17(severity_set: pl.LazyFrame) -> pl.LazyFrame:
    """Model Training 17 node"""
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
