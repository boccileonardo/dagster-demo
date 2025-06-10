import dagster as dg
import polars as pl
from dagster_polars import PolarsDeltaIOManager
from dagster_demo.components.constants import LANDING_ZONE
from dagster_demo.components.refinement import (
    silver_fct_processing,
    silver_prod_dim_processing,
    silver_site_dim_processing,
    silver_fct_downsample,
)

RETAILER_ID = "1001"
DIRECTORY = f"{LANDING_ZONE}/{RETAILER_ID}/"


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    group_name="carretwo",
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_carretwo_day_fct(
    context: dg.AssetExecutionContext, bronze_carretwo_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = bronze_carretwo_day_fct.select(
        pl.col("date").alias("time_period_end_date").str.to_date("%Y-%m-%d"),
        pl.col("store").alias("store_id"),
        pl.col("product").alias("prod_id"),
        pl.col("sales_qty").alias("pos_sales_units"),
        pl.col("sales_value_usd").alias("pos_sales_value"),
        pl.col("ingested_at_utc_datetime"),
        pl.col("ingested_at_date"),
    )
    df = silver_fct_processing(df)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    group_name="carretwo",
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_carretwo_prod_dim(
    context: dg.AssetExecutionContext, bronze_carretwo_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = bronze_carretwo_day_fct.select(pl.col("product").alias("prod_id")).unique()
    df = silver_prod_dim_processing(df)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    group_name="carretwo",
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_carretwo_site_dim(
    context: dg.AssetExecutionContext, bronze_carretwo_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = bronze_carretwo_day_fct.select(pl.col("store").alias("site_id")).unique()
    df = silver_site_dim_processing(df)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    group_name="carretwo",
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_carretwo_week_fct(
    context: dg.AssetExecutionContext, silver_carretwo_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(silver_carretwo_day_fct, sampling_period="1w")
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    group_name="carretwo",
    automation_condition=dg.AutomationCondition.eager(),
)
def silver_carretwo_month_fct(
    context: dg.AssetExecutionContext, silver_carretwo_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(silver_carretwo_day_fct, "1mo")
    return df


defs = dg.Definitions(
    assets=[
        silver_carretwo_day_fct,
        silver_carretwo_prod_dim,
        silver_carretwo_site_dim,
        silver_carretwo_week_fct,
        silver_carretwo_month_fct,
    ],
    resources={
        "silver_polars_parquet_io_manager": PolarsDeltaIOManager(
            base_dir="data/bronze", mode="overwrite"
        )
    },
)
