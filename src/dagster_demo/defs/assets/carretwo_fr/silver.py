import dagster as dg
import polars as pl
from dagster_demo.defs.assets.carretwo_fr import config as cfg
from dagster_demo.components.silver import (
    silver_fact_processing,
    silver_prod_dim_processing,
    silver_site_dim_processing,
    silver_fact_downsample,
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
)  # type: ignore[call-overload]
def carretwo_fr_silver_day_fact(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fact.select(
        pl.col("date").str.to_date("%Y-%m-%d").alias("time_period_end_date"),
        pl.col("product_id").cast(pl.Int32).alias("prod_id"),
        pl.col("store_id").cast(pl.Int32).alias("site_id"),
        pl.col("sales_qty").cast(pl.Int64).alias("pos_sales_units"),
        pl.col("sales_value_usd").cast(pl.Float64).alias("pos_sales_value_usd"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_fact_processing(context=context, df=df)
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
)  # type: ignore[call-overload]
def carretwo_fr_silver_prod_dim(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fact.select(
        pl.col("product_id").cast(pl.Int32).alias("prod_id"),
        pl.col("product").alias("prod_name"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_prod_dim_processing(context=context, df=df)
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
)  # type: ignore[call-overload]
def carretwo_fr_silver_site_dim(
    context: dg.AssetExecutionContext, carretwo_fr_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = carretwo_fr_bronze_day_fact.select(
        pl.col("store_id").cast(pl.Int32).alias("site_id"),
        pl.col("store").alias("site_name"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_site_dim_processing(context=context, df=df)
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
)  # type: ignore[call-overload]
def carretwo_fr_silver_week_fact(
    context: dg.AssetExecutionContext, carretwo_fr_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context, df=carretwo_fr_silver_day_fact, sampling_period="1w"
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
)  # type: ignore[call-overload]
def carretwo_fr_silver_month_fact(
    context: dg.AssetExecutionContext, carretwo_fr_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context,
        df=carretwo_fr_silver_day_fact,
        sampling_period="1mo",
    )
    return df


defs = dg.Definitions(
    assets=[
        carretwo_fr_silver_day_fact,
        carretwo_fr_silver_week_fact,
        carretwo_fr_silver_month_fact,
        carretwo_fr_silver_prod_dim,
        carretwo_fr_silver_site_dim,
    ],
)
