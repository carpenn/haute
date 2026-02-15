"""My first pricing pipeline."""

import polars as pl
import runw

pipeline = runw.Pipeline("my_project", description="A new pricing pipeline")


@pipeline.node(path="data/input.parquet")
def read_data() -> pl.DataFrame:
    """Read input data."""
    return pl.read_parquet("data/input.parquet")


@pipeline.node
def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Transform the data."""
    return df


@pipeline.node
def output(df: pl.DataFrame) -> pl.DataFrame:
    """Final output."""
    return df
