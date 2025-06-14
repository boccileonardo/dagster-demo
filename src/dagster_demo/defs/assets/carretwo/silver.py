import dagster as dg
import polars as pl
from dagster_demo.defs.assets.carretwo import config as cfg
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
    kinds=["polars", "deltalake", "silver"],
)
def carretwo_fr_silver_day_fct(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fct.select(
        pl.col("date").alias("time_period_end_date").str.to_date("%Y-%m-%d"),
        pl.col("store").alias("site_id"),
        pl.col("product").alias("prod_id"),
        pl.col("sales_qty").alias("pos_sales_units"),
        pl.col("sales_value_usd").alias("pos_sales_value"),
        pl.col("ingested_at_utc_datetime"),
        pl.col("ingested_at_date"),
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
    kinds=["polars", "deltalake", "silver"],
)
def carretwo_fr_silver_prod_dim(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fct.select(pl.col("product").alias("prod_id")).unique()
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
    kinds=["polars", "deltalake", "silver"],
)
def carretwo_fr_silver_site_dim(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fct.select(pl.col("store").alias("site_id")).unique()
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
    kinds=["polars", "deltalake", "silver"],
    tags={"aggregation": "day_to_week"},
)
def carretwo_fr_silver_week_fct(
    context: dg.AssetExecutionContext, carretwo_fr_silver_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(
        context=context, df=carretwo_fr_silver_day_fct, sampling_period="1w", config=cfg
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
    kinds=["polars", "deltalake", "silver"],
    tags={"aggregation": "day_to_month"},
)
def carretwo_fr_silver_month_fct(
    context: dg.AssetExecutionContext, carretwo_fr_silver_day_fct: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fct_downsample(
        context=context,
        df=carretwo_fr_silver_day_fct,
        sampling_period="1mo",
        config=cfg,
    )
    return df


defs = dg.Definitions(
    assets=[
        carretwo_fr_silver_day_fct,
        carretwo_fr_silver_week_fct,
        carretwo_fr_silver_month_fct,
        carretwo_fr_silver_prod_dim,
        carretwo_fr_silver_site_dim,
    ],
)
