import dagster as dg
import polars as pl
from dagster_demo.defs.assets.lidlo_de import config as cfg
from dagster_demo.components.silver import (
    silver_fact_processing,
    silver_prod_dim_processing,
    silver_site_dim_processing,
    silver_fact_downsample,
)


@dg.asset(
    io_manager_key="silver_polars_delta_merge_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "merge_predicate": "s.time_period_end_date = t.time_period_end_date AND s.prod_id = t.prod_id AND s.site_id = t.site_id",
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_day_fact(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fact.select(
        pl.col("date").str.to_date("%Y-%m-%d").alias("time_period_end_date"),
        pl.col("product_id").cast(pl.Int32).alias("prod_id"),
        pl.col("store_id").cast(pl.Int32).alias("site_id"),
        pl.col("sales_qty").cast(pl.Int64).alias("pos_sales_units"),
        pl.col("sales_value_usd").cast(pl.Float64).alias("pos_sales_value_usd"),
        pl.col("sales_value_local_currency")
        .cast(pl.Float64)
        .alias("pos_sales_value_lc"),
        pl.col("return_amount").cast(pl.Int64).alias("returned_qty"),
        pl.col("return_value_local_currency")
        .cast(pl.Float64)
        .alias("returned_value_lc"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_fact_processing(context=context, df=df)
    return df


@dg.asset(
    io_manager_key="silver_polars_delta_merge_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "merge_predicate": "s.prod_id = t.prod_id",
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_prod_dim(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fact.select(
        pl.col("product_id").cast(pl.Int32).alias("prod_id"),
        pl.col("product").alias("prod_name"),
        pl.col("category"),
        pl.col("sector"),
        pl.col("launch_date").str.to_date("%Y-%m-%d").alias("prod_launch_date"),
        pl.col("GTIN").cast(pl.Int64).alias("item_gtin"),
        pl.col("description").alias("item_description"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_prod_dim_processing(context=context, df=df)
    return df


@dg.asset(
    io_manager_key="silver_polars_delta_merge_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "merge_predicate": "s.site_id = t.site_id",
    },
    kinds={"polars", "deltalake", "silver"},
)
def lidlo_de_silver_site_dim(
    context: dg.AssetExecutionContext, lidlo_de_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = lidlo_de_bronze_day_fact.select(
        pl.col("store_id").cast(pl.Int32).alias("site_id"),
        pl.col("store").alias("site_name"),
        pl.col("city"),
        pl.col("address"),
        pl.col("channel"),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("global_location_number").cast(pl.Int64),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
    )
    df = silver_site_dim_processing(context=context, df=df)
    return df


@dg.asset(
    io_manager_key="silver_polars_delta_merge_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "merge_predicate": "s.time_period_end_date = t.time_period_end_date AND s.prod_id = t.prod_id AND s.site_id = t.site_id",
    },
    kinds={"polars", "deltalake", "silver"},
    tags={"aggregation": "day_to_week"},
)
def lidlo_de_silver_week_fact(
    context: dg.AssetExecutionContext, lidlo_de_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context, df=lidlo_de_silver_day_fact, sampling_period="1w"
    )
    return df


@dg.asset(
    io_manager_key="silver_polars_delta_merge_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "merge_predicate": "s.time_period_end_date = t.time_period_end_date AND s.prod_id = t.prod_id AND s.site_id = t.site_id",
    },
    kinds={"polars", "deltalake", "silver"},
    tags={"aggregation": "day_to_month"},
)
def lidlo_de_silver_month_fact(
    context: dg.AssetExecutionContext, lidlo_de_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context, df=lidlo_de_silver_day_fact, sampling_period="1mo"
    )
    return df


defs = dg.Definitions(
    assets=[
        lidlo_de_silver_day_fact,
        lidlo_de_silver_prod_dim,
        lidlo_de_silver_site_dim,
        lidlo_de_silver_week_fact,
        lidlo_de_silver_month_fact,
    ],
)
