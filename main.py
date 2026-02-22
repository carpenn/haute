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


@pipeline.node(live_switch=True, mode="live")
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return quotes


@pipeline.node(factors=[{'banding': 'continuous', 'column': 'DrivAge', 'output_column': 'DrivAgeBand', 'rules': [{'op1': '>', 'val1': '0', 'op2': '<=', 'val2': '20', 'assignment': '0-20'}, {'op1': '>', 'val1': '20', 'op2': '<=', 'val2': '30', 'assignment': '20-30'}, {'op1': '>', 'val1': '30', 'op2': '<=', 'val2': '40', 'assignment': '30-40'}, {'op1': '>', 'val1': '40', 'op2': '<=', 'val2': '50', 'assignment': '50-60'}, {'op1': '>', 'val1': '50', 'op2': '', 'val2': '', 'assignment': '60+'}]}, {'banding': 'categorical', 'column': 'Region', 'output_column': 'RegionBand', 'rules': [{'value': 'Centre', 'assignment': 'Centre'}, {'value': 'London', 'assignment': 'London'}, {'value': 'Paris', 'assignment': 'Paris'}], 'default': 'Other'}, {'banding': 'continuous', 'column': 'VehPower', 'output_column': 'VehPowerBand', 'rules': [{'op1': '>=', 'val1': '0', 'op2': '<', 'val2': '3', 'assignment': '0-3'}, {'op1': '>=', 'val1': '3', 'op2': '<', 'val2': '6', 'assignment': '3-6'}, {'op1': '>=', 'val1': '6', 'op2': '', 'val2': '', 'assignment': '6+'}]}])
def Banding_15(policies: pl.LazyFrame) -> pl.LazyFrame:
    """Banding 15 node"""
    return df


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


@pipeline.node(tables=[{'name': 'Multit', 'factors': ['DrivAgeBand', 'RegionBand', 'VehPowerBand'], 'output_column': 'Multi', 'entries': [{'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '0-20', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '20-30', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '30-40', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '50-60', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '3-6', 'value': 2}, {'DrivAgeBand': '60+', 'RegionBand': 'Centre', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'London', 'VehPowerBand': '6+', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '0-3', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '3-6', 'value': 1}, {'DrivAgeBand': '60+', 'RegionBand': 'Paris', 'VehPowerBand': '6+', 'value': 1}], 'default_value': '1.0'}])
def Rating_Step_16(Banding_15: pl.LazyFrame) -> pl.LazyFrame:
    """Rating Step 16 node"""
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
pipeline.connect("policies", "Banding_15")
pipeline.connect("Banding_15", "Rating_Step_16")
pipeline.connect("severity_set", "Model_Training_17")
pipeline.connect("policies", "Model_Score_19")
