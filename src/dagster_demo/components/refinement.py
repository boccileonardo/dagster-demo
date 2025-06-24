import polars as pl
import polars.selectors as cs
import dagster as dg
from typing import Literal
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.ingestion import add_ingestion_metadata


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
    df = df.unique(subset=["prod_id"])
    # TODO: dagster asset checks, example join with corporate master data
    add_materialization_metadata(context=context, df=df)
    return df


def silver_site_dim_processing(
    context: dg.AssetExecutionContext, df: pl.LazyFrame, config
):
    df = df.unique(subset=["site_id"])
    # TODO: dagster asset checks, example join with corporate master data
    add_materialization_metadata(context=context, df=df)
    return df


def silver_fct_downsample(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    sampling_period: Literal["1w", "1mo"],
):
    """Return a dataframe with aggregate granularity.

    Args:
        df (pl.LazyFrame): more granular dataframe
        sampling_period ("1w" OR "1mo"): aggregation level for dates
    """
    df = df.sort("time_period_end_date")
    df = df.group_by_dynamic(
        index_column="time_period_end_date",
        group_by=["prod_id", "site_id"],
        every=sampling_period,
        label="right",
    ).agg(
        cs.numeric().sum(),
    )
    df = add_ingestion_metadata(df=df, data_source="aggregation")
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df
