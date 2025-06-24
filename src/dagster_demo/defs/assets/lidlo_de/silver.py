import dagster as dg
import polars as pl
from dagster_demo.defs.assets.lidlo_de import config as cfg
from dagster_demo.components.refinement import (
    silver_fct_processing,
    silver_prod_dim_processing,
    silver_site_dim_processing,
    silver_fct_downsample,
)


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_day_fct(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fct.select(
        pl.col("date").alias("time_period_end_date").str.to_date("%Y-%m-%d"),
        pl.col("store").alias("site_id"),
        pl.col("product").alias("prod_id"),
        pl.col("sales_qty").alias("pos_sales_units"),
        pl.col("sales_value_usd").alias("pos_sales_value"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
    )
    df = silver_fct_processing(context=context, df=df, config=cfg)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_prod_dim(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fct.select(
        pl.col("product").alias("prod_id"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
    )
    df = silver_prod_dim_processing(context=context, df=df, config=cfg)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_site_dim(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fct.select(
        pl.col("store").alias("site_id"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
    )
    df = silver_site_dim_processing(context=context, df=df, config=cfg)
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "silver"},
    tags={"aggregation": "day_to_week"},
)
def lidlo_de_silver_week_fct(
    context: dg.AssetExecutionContext, lidlo_de_silver_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(
        context=context, df=lidlo_de_silver_day_fct, sampling_period="1w"
    )
    return df


@dg.asset(
    io_manager_key="silver_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "silver"},
    tags={"aggregation": "day_to_month"},
)
def lidlo_de_silver_month_fct(
    context: dg.AssetExecutionContext, lidlo_de_silver_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(
        context=context, df=lidlo_de_silver_day_fct, sampling_period="1mo"
    )
    return df


defs = dg.Definitions(
    assets=[
        lidlo_de_silver_day_fct,
        lidlo_de_silver_prod_dim,
        lidlo_de_silver_site_dim,
        lidlo_de_silver_week_fct,
        lidlo_de_silver_month_fct,
    ],
)
