"""Fixture pipeline for tests — self-contained, no external model dependencies.

This pipeline mirrors the structure of a real pricing pipeline (apiInput,
dataSource, liveSwitch, transform, externalFile, output, dataSink) but uses
only simple Polars expressions and a JSON lookup file so tests don't depend
on CatBoost or the user's main.py.
"""

import polars as pl

import haute

pipeline = haute.Pipeline("test_pipeline", description="Test fixture pipeline")


@pipeline.node(api_input=True, path="tests/fixtures/data/api_input.json", row_id_column="IDpol")
def quotes() -> pl.LazyFrame:
    """API input source."""
    return pl.read_json("tests/fixtures/data/api_input.json").lazy()


@pipeline.node(path="tests/fixtures/data/policies.parquet")
def batch_quotes() -> pl.LazyFrame:
    """Batch data source."""
    return pl.scan_parquet("tests/fixtures/data/policies.parquet")


@pipeline.node(live_switch=True, mode="live")
def policies(quotes: pl.LazyFrame, batch_quotes: pl.LazyFrame) -> pl.LazyFrame:
    """Live/batch switch."""
    return quotes


@pipeline.node(external="tests/fixtures/data/area_factors.json", file_type="json")
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


@pipeline.node
def calculate_premium(area_lookup: pl.LazyFrame) -> pl.LazyFrame:
    """Simple premium calculation."""
    df = area_lookup.with_columns(
        premium=(pl.col("VehPower") * pl.col("area_factor") * pl.col("Exposure")),
    )
    return df


@pipeline.node(output=True)
def output(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Output node."""
    return calculate_premium


@pipeline.node(sink="tests/fixtures/output/results.parquet", format="parquet")
def results_write(calculate_premium: pl.LazyFrame) -> pl.LazyFrame:
    """Sink node."""
    return calculate_premium


pipeline.connect("quotes", "policies")
pipeline.connect("batch_quotes", "policies")
pipeline.connect("policies", "area_lookup")
pipeline.connect("area_lookup", "calculate_premium")
pipeline.connect("calculate_premium", "output")
pipeline.connect("calculate_premium", "results_write")
