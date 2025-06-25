import polars as pl
import polars.selectors as cs
import dagster as dg
from typing import Literal
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.bronze import add_ingestion_metadata


def load_product_master_data():
    df = pl.scan_parquet("faker/data/corporate_product_master_data.parquet")
    col_names = df.collect_schema().names()
    prefixed_col_names = [
        f"corp_{col}" if col != "item_gtin" else col for col in col_names
    ]
    return df.rename(
        {col: new_col for col, new_col in zip(col_names, prefixed_col_names)}
    )


def load_site_master_data():
    df = pl.scan_parquet("faker/data/corporate_site_master_data.parquet")
    col_names = df.collect_schema().names()
    prefixed_col_names = [
        f"corp_{col}" if col != "global_location_number" else col for col in col_names
    ]
    return df.rename(
        {col: new_col for col, new_col in zip(col_names, prefixed_col_names)}
    )


def silver_fct_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique()  # deduplication of bronze data
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df


def silver_prod_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["prod_id"])
    master = load_product_master_data()
    df = df.join(
        master,
        left_on="prod_id",
        right_on="item_gtin",
        how="left",
    )
    add_materialization_metadata(context=context, df=df)
    return df


def silver_site_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["site_id"])
    master = load_site_master_data()
    df = df.join(
        master,
        left_on="site_id",
        right_on="global_location_number",
        how="left",
    )
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
