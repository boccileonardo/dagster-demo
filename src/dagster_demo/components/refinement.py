import polars as pl
from typing import Literal


def silver_fct_processing(df: pl.LazyFrame):
    df = df.unique()  # deduplication of bronze data
    # TODO: dagster asset checks
    return df


def silver_prod_dim_processing(df: pl.LazyFrame):
    # TODO: dagster asset checks, example join with corporate master data
    return df


def silver_site_dim_processing(df: pl.LazyFrame):
    # TODO: dagster asset checks, example join with corporate master data
    return df


def silver_fct_downsample(df: pl.LazyFrame, sampling_period: Literal["1w", "1mo"]):
    df = (
        df.group_by_dynamic(
            "time_period_end_date", every=sampling_period, label="right"
        )
        .agg()
        .sum()
    )
    # TODO: use polars selectors to apply good aggregations to each column
    return df
