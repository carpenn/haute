"""Pipeline: my_pipeline"""

import polars as pl
import haute

from helpers.features import (
    addon_features,
    driver_features,
    to_date,
    years_between,
    ADDON_NAMES,
    DERIVED_COLS,
    RENAME_MAP,
)

pipeline = haute.Pipeline("my_pipeline", description="")


@pipeline.node(config="config/datasource/batch_quotes.json")
def batch_quotes() -> pl.LazyFrame:
    """batch_quotes node"""
    return pl.scan_parquet("data/batch_quotes.parquet")


@pipeline.node(config="config/datasource/competitor_insights.json")
def competitor_insights() -> pl.LazyFrame:
    """competitor_insights node"""
    return pl.scan_parquet("data/competitor_insight.parquet")


@pipeline.node(config="config/api_input/quotes.json")
def quotes() -> pl.LazyFrame:
    """api_input node"""
    from haute._json_flatten import read_json_flat
    return read_json_flat("data/quotes_10m.jsonl", config_path="config/api_input/quotes.json")


@pipeline.node
def feature_processing(quotes: pl.LazyFrame) -> pl.LazyFrame:
    """Feature engineering for insurance pricing"""
    cols = quotes.collect_schema().names()
    cover_start = to_date("policy_details.cover_start_date")
    
    # Helpers build the dynamic driver + addon expressions
    ad_derived, ad_age_cols, ad_renames, ad_keep = driver_features(cols, cover_start)
    addon_derived, addon_renames, addon_keep = addon_features(cols, ADDON_NAMES)
    
    # Step 1 — Add calculated columns
    df = quotes.with_columns(
        # Proposer
        years_between(to_date("proposer.date_of_birth"), cover_start).alias("proposer_age"),
        years_between(to_date("proposer.licence.licence_date"), cover_start).alias("proposer_licence_length_years"),
        # Vehicle
        (cover_start.dt.year() - pl.col("vehicle.year_of_manufacture")).alias("vehicle_age"),
        # Policy
        (pl.col("policy_details.voluntary_excess") + pl.col("policy_details.compulsory_excess")).alias("total_excess"),
        # Address
        (pl.col("address.years_at_address") * 12 + pl.col("address.months_at_address")).alias("address_total_months"),
        pl.col("address.postcode").str.split(" ").list.first().alias("postcode_area"),
        # Additional drivers + counts + addons
        *ad_derived,
        *addon_derived,
    )
    
    # Step 2 — Youngest driver across proposer + any additional drivers
    df = df.with_columns(
        pl.min_horizontal("proposer_age", *ad_age_cols).alias("youngest_driver_age"),
    )
    
    # Step 3 — Rename dot-notation columns to clean names
    df = df.rename({**RENAME_MAP, **ad_renames, **addon_renames})
    
    # Step 4 — Keep only the columns we need
    df = df.select(list(RENAME_MAP.values()) + DERIVED_COLS + ad_keep + addon_keep)
     
    df
    return df


@pipeline.node(config="config/live_switch/policies.json")
def policies(feature_processing: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """policies node"""
    return batch_quotes


@pipeline.node
def competitor_join(policies: pl.LazyFrame, competitor_insights: pl.LazyFrame) -> pl.LazyFrame:
    """competitor_join node"""
    df = (
    policies
    .join(
        competitor_insights, 
        on = 'quote_id', 
        how = 'inner'
    )
    )
    return df


@pipeline.node(config="config/modelling/avg_top_5.json")
def avg_top_5(competitor_join: pl.LazyFrame) -> pl.LazyFrame:
    """avg_top_5 node"""
    return df



# Wire nodes together - edges define data flow
pipeline.connect("quotes", "feature_processing")
pipeline.connect("feature_processing", "policies")
pipeline.connect("batch_quotes", "policies")
pipeline.connect("policies", "competitor_join")
pipeline.connect("competitor_insights", "competitor_join")
pipeline.connect("competitor_join", "avg_top_5")
