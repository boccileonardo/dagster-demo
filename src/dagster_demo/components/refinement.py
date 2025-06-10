import polars as pl
import dagster as dg
from typing import Literal
from dagster_demo.components.output_metadata import add_materialization_metadata


def silver_fct_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame, config):
    df = df.unique()  # deduplication of bronze data
    # TODO: dagster asset checks
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df


def silver_prod_dim_processing(
    context: dg.AssetExecutionContext, df: pl.LazyFrame, config
):
    # TODO: dagster asset checks, example join with corporate master data
    add_materialization_metadata(context=context, df=df)
    return df


def silver_site_dim_processing(
    context: dg.AssetExecutionContext, df: pl.LazyFrame, config
):
    # TODO: dagster asset checks, example join with corporate master data
    add_materialization_metadata(context=context, df=df)
    return df


def silver_fct_downsample(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    sampling_period: Literal["1w", "1mo"],
    config,
):
    df = df.sort("time_period_end_date")
    df = (
        df.group_by_dynamic(
            "time_period_end_date", every=sampling_period, label="right"
        )
        .agg()
        .sum()
    )
    # TODO: use polars selectors to apply good aggregations to each column
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df
