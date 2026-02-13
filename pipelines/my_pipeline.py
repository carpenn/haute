"""Pipeline: my_pipeline"""

import polars as pl
import runw

pipeline = runw.Pipeline("my_pipeline", description="")


@pipeline.node(path="examples/data/frequency_set.parquet")
def data_source() -> pl.DataFrame:
    """data_source node"""
    return pl.read_parquet("examples/data/frequency_set.parquet")


@pipeline.node
def AvgAge(data_source: pl.DataFrame) -> pl.DataFrame:
    """AvgAge node"""
    df = (
    data_source
        .group_by('DrivAge')
        .agg(
            pl.mean('VehAge').alias('AvgAge'),
            pl.count('VehAge').alias('Count')
        )
    )
    return df


@pipeline.node
def Region(data_source: pl.DataFrame) -> pl.DataFrame:
    """Region node"""
    df = (
        data_source.with_columns(regions2=pl.col('Region'))
    )
    return df


@pipeline.node
def Joined(Region: pl.DataFrame, AvgAge: pl.DataFrame) -> pl.DataFrame:
    """Joined node"""
    df = (
        Region
        .join(
            AvgAge, 
            on = 'DrivAge', 
            how = 'left'
        )
    )
    return df



# Wire nodes together — edges define data flow
pipeline.connect("data_source", "Region")
pipeline.connect("data_source", "AvgAge")
pipeline.connect("Region", "Joined")
pipeline.connect("AvgAge", "Joined")
