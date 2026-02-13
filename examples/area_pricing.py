"""Area pricing pipeline — simple example."""

import polars as pl
import runw

pipeline = runw.Pipeline("area_pricing", description="Assign premium based on area code")


@pipeline.node(path="examples/data/frequency_set.parquet")
def read_data() -> pl.DataFrame:
    """Read policy data from parquet."""
    return pl.read_parquet("examples/data/frequency_set.parquet")


@pipeline.node
def area_premium(df: pl.DataFrame) -> pl.DataFrame:
    """Assign premium based on Area column."""
    return df.with_columns(
        pl.when(pl.col("Area") == "A").then(100)
          .when(pl.col("Area") == "B").then(200)
          .otherwise(300)
          .alias("premium")
    )


@pipeline.node
def output(df: pl.DataFrame) -> pl.DataFrame:
    """Final prediction output."""
    return df.select("IDpol", "Area", "premium")
