"""Fixture pipeline for tests — self-contained, no external model dependencies.

This pipeline mirrors the structure of a real pricing pipeline (apiInput,
dataSource, liveSwitch, polars, externalFile, output, dataSink) but uses
only simple Polars expressions and a JSON lookup file so tests don't depend
on CatBoost or the user's main.py.
"""

import polars as pl

import haute

pipeline = haute.Pipeline("test_pipeline", description="Test fixture pipeline")


@pipeline.api_input(path="tests/fixtures/data/api_input.json", row_id_column="IDpol")
def quotes() -> pl.LazyFrame:
    """API input source."""
    return pl.read_json("tests/fixtures/data/api_input.json").lazy()


@pipeline.data_source(path="tests/fixtures/data/policies.parquet")
def batch_quotes() -> pl.LazyFrame:
    """Batch data source."""
    return pl.scan_parquet("tests/fixtures/data/policies.parquet")


@pipeline.live_switch(input_scenario_map={"quotes": "live", "batch_quotes": "test_batch"})
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """Live/batch switch."""
    return quotes


@pipeline.external_file(path="tests/fixtures/data/area_factors.json", file_type="json")
def area_lookup(policies: pl.LazyFrame) -> pl.LazyFrame:
    """External file node — loads a JSON lookup table.

    The executor injects ``obj`` (the loaded JSON) via extra_ns.
    The parser strips the import + load_external_object call from the
    code body — only the lines that use ``obj`` are kept.
    """
    from haute.graph_utils import load_external_object

    obj = load_external_object("tests/fixtures/data/area_factors.json", "json")
    df = policies.with_columns(
        area_factor=pl.col("Area").replace_strict(obj, default=1.0),
    )
    return df


@pipeline.polars
def calculate_premium(area_lookup: pl.LazyFrame) -> pl.LazyFrame:
    """Simple premium calculation."""
    df = area_lookup.with_columns(
        premium=(pl.col("VehPower") * pl.col("area_factor") * pl.col("Exposure")),
    )
    return df


@pipeline.output()
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output node."""
    return calculate_premium


@pipeline.data_sink(path="tests/fixtures/output/results.parquet", format="parquet")
def results_write(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Sink node."""
    return calculate_premium


pipeline.connect("quotes", "policies")
pipeline.connect("batch_quotes", "policies")
pipeline.connect("policies", "area_lookup")
pipeline.connect("area_lookup", "calculate_premium")
pipeline.connect("calculate_premium", "output")
pipeline.connect("calculate_premium", "results_write")
