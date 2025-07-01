import polars as pl
import polars.selectors as cs
import dagster as dg
from typing import Literal
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.bronze import add_ingestion_metadata
from dagster_demo.components.polars_schemas import (
    prod_dim_required_cols,
    site_dim_required_cols,
    store_fact_required_cols,
)


def prefix_cols(df: pl.LazyFrame, prefix: str):
    col_names = df.collect_schema().names()
    technical_columns = set(
        prod_dim_required_cols + site_dim_required_cols + store_fact_required_cols
    )
    prefixed_col_names = [
        f"{prefix}_{col}" if col not in technical_columns else col for col in col_names
    ]
    return df.rename(
        {col: new_col for col, new_col in zip(col_names, prefixed_col_names)}
    )


def load_product_master_data():
    df = pl.scan_parquet("faker/data/corporate_product_master_data.parquet")
    return prefix_cols(df, "corp")


def load_site_master_data():
    df = pl.scan_parquet("faker/data/corporate_site_master_data.parquet")
    return prefix_cols(df, "corp")


def silver_fact_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique()  # deduplication of bronze data
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df


def silver_prod_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["prod_id"])
    df = prefix_cols(df, "source")
    if "source_item_gtin" in df.collect_schema().names():
        master = load_product_master_data()
        df = df.join(
            master,
            left_on="source_item_gtin",
            right_on="corp_item_gtin",
            how="left",
        )
    add_materialization_metadata(context=context, df=df)
    return df


def silver_site_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["site_id"])
    df = prefix_cols(df, "source")
    if "source_global_location_number" in df.collect_schema().names():
        master = load_site_master_data()
        df = df.join(
            master,
            left_on="source_global_location_number",
            right_on="corp_global_location_number",
            how="left",
        )
    add_materialization_metadata(context=context, df=df)
    return df


def silver_fact_downsample(
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
        group_by=["prod_id", "site_id", "data_provider_code"],
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
