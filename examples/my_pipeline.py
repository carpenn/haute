"""Pipeline: my_pipeline"""

import polars as pl
import runw

pipeline = runw.Pipeline("my_pipeline", description="")


@pipeline.node(path="examples/data/frequency_set.parquet")
def read_data() -> pl.DataFrame:
    """Read policy data from parquet."""
    return pl.read_parquet("examples/data/frequency_set.parquet")


@pipeline.node
def area_premium(df: pl.DataFrame) -> pl.DataFrame:
    """Assign premium based on Area column."""
    # TODO: implement transform logic
    return df


@pipeline.node
def output(df: pl.DataFrame) -> pl.DataFrame:
    """Final prediction output."""
    return df


@pipeline.node(path="")
def data_source_4() -> pl.DataFrame:
    """Data Source 4 node"""
    return pl.read_parquet("")

