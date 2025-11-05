import polars as pl
import polars.selectors as cs
import dagster as dg
from typing import Literal
from dagster_demo.components.logger import logger
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.bronze import add_ingestion_metadata
from dagster_demo.components.polars_schemas import (
    prod_dim_pl_schema,
    site_dim_pl_schema,
)
from dagster_demo.components.polars_schemas import (
    prod_dim_required_cols,
    site_dim_required_cols,
    store_fact_required_cols,
)


def prefix_cols(df: pl.LazyFrame, prefix: str, exclude: list[str] = []):
    col_names = df.collect_schema().names()
    excluded_columns = set(
        prod_dim_required_cols
        + site_dim_required_cols
        + store_fact_required_cols
        + exclude
    )
    prefixed_col_names = [
        f"{prefix}_{col}" if col not in excluded_columns else col for col in col_names
    ]
    return df.rename(
        {col: new_col for col, new_col in zip(col_names, prefixed_col_names)}
    )


def load_product_master_data():
    df = pl.scan_parquet("faker/data/corporate_product_master_data.parquet")
    return prefix_cols(df, prefix="corp")


def load_site_master_data():
    df = pl.scan_parquet("faker/data/corporate_site_master_data.parquet")
    return prefix_cols(df, prefix="corp")


def column_name_is_in_data_model(table: str, col_name) -> bool:
    """Ensure the column name that is being mapped as non-standard is (not) in the data model.
    Guards against schema evolution (promoted fields from extra_attributes)."""
    match table:
        case "store_fact":
            return col_name in site_dim_pl_schema
        case "prod_dim":
            return col_name in prod_dim_pl_schema
        case "site_dim":
            return col_name in site_dim_pl_schema
        case _:
            # add table support to this function if hitting this error
            raise NotImplementedError("This table is not yet supported for validation.")


def silver_fact_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique()  # deduplication of bronze data
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df


def silver_prod_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["prod_id"])
    df = prefix_cols(df, prefix="source", exclude=["extra_attributes"])
    if "source_item_gtin" in df.collect_schema().names():
        logger.info("Joining with corporate product master on GTIN.")
        master = load_product_master_data()
        df = df.join(
            master,
            left_on="source_item_gtin",
            right_on="corp_item_gtin",
            how="left",
        )
    else:
        logger.info(
            "Skipping join with corporate master data due to missing GTIN for this retailer."
        )
    df = df.with_columns(
        prod_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "prod_id"
        ).cast(pl.Int64)
    )
    add_materialization_metadata(
        context=context, df=df, count_rows=False, count_ids_in_col="prod_id"
    )
    return df


def silver_site_dim_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame):
    df = df.unique(subset=["site_id"])
    df = prefix_cols(df, prefix="source", exclude=["extra_attributes"])
    if "source_global_location_number" in df.collect_schema().names():
        master = load_site_master_data()
        df = df.join(
            master,
            left_on="source_global_location_number",
            right_on="corp_global_location_number",
            how="left",
        )
    else:
        logger.warning(
            "Unable to join with site master data. global_location_number not found."
        )
    df = df.with_columns(
        site_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "site_id"
        ).cast(pl.Int64)
    )
    add_materialization_metadata(
        context=context, df=df, count_rows=False, count_ids_in_col="site_id"
    )
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
        context=context,
        df=df,
        count_dates_in_col="time_period_end_date",
        count_rows=False,
    )
    return df
