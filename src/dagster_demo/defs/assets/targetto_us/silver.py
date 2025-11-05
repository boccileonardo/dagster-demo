import dagster as dg
import polars as pl
from dagster_demo.defs.assets.targetto_us import config as cfg
from dagster_demo.components.silver import column_name_is_in_data_model
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
def targetto_us_silver_day_fact(
    context: dg.AssetExecutionContext, targetto_us_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    non_standard_fact_cols = [
        "safety_stock_qty",
        "cycle_count_variance_qty",
        "inventory_accuracy_pct",
        "days_of_supply",
        "shrinkage_qty",
        "reserved_qty_for_online",
        "backroom_qty",
    ]
    for col in non_standard_fact_cols:
        if column_name_is_in_data_model(table="store_fact", col_name=col):
            raise ValueError(
                f"Column {col} exists in the standard data model and should be mapped directly."
            )
    df = targetto_us_bronze_day_fact.select(
        pl.col("date").str.to_date("%Y-%m-%d").alias("time_period_end_date"),
        pl.col("product_id").cast(pl.Int64).alias("prod_id"),
        pl.col("store_id").cast(pl.Int64).alias("site_id"),
        pl.col("inventory_qty").cast(pl.Int64).alias("inventory_qty_on_hand"),
        pl.col("units_on_order").cast(pl.Int64).alias("inventory_qty_on_order"),
        pl.col("value_on_hand_usd")
        .cast(pl.Float64)
        .alias("inventory_value_on_hand_usd"),
        pl.col("value_on_hand_local_currency")
        .cast(pl.Float64)
        .alias("inventory_value_on_hand_lc"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
        # non standard columns into extra attrs
        pl.struct([pl.col(c) for c in non_standard_fact_cols]).alias(
            "extra_attributes"
        ),
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
def targetto_us_silver_prod_dim(
    context: dg.AssetExecutionContext, targetto_us_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    non_standard_prod_dim_cols = [
        "product_id",
        "logo_present_on_pack",
        "package_material",
        "shelf_life_days",
        "is_bundle",
        "brand_marketing_tier",
        "num_components",
    ]
    for col in non_standard_prod_dim_cols:
        if column_name_is_in_data_model(table="prod_dim", col_name=col):
            raise ValueError(
                f"Column {col} exists in the standard data model and should be mapped directly."
            )
    df = targetto_us_bronze_day_fact.select(
        pl.col("product_id").cast(pl.Int64).alias("prod_id"),
        pl.col("product").alias("prod_name"),
        pl.col("sector").alias("sector"),
        pl.col("category").alias("category"),
        pl.col("GTIN").cast(pl.Int64).alias("item_gtin"),
        pl.col("description").alias("item_description"),
        pl.col("launch_date").str.to_date("%Y-%m-%d").alias("prod_launch_date"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
        # non standard columns into extra attrs
        pl.struct([pl.col(c) for c in non_standard_prod_dim_cols]).alias(
            "extra_attributes"
        ),
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
def targetto_us_silver_site_dim(
    context: dg.AssetExecutionContext, targetto_us_bronze_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    non_standard_site_dim_cols = [
        "store_id",
        "store_manager_name",
        "store_opening_date",
        "format_subtype",
        "store_square_footage",
        "lease_type",
        "parking_spaces",
        "micro_fulfillment_enabled",
    ]
    for col in non_standard_site_dim_cols:
        if column_name_is_in_data_model(table="site_dim", col_name=col):
            raise ValueError(
                f"Column {col} exists in the standard data model and should be mapped directly."
            )
    df = targetto_us_bronze_day_fact.select(
        pl.col("store_id").cast(pl.Int64).alias("site_id"),
        pl.col("store").alias("site_name"),
        pl.col("global_location_number").cast(pl.Int64).alias("global_location_number"),
        pl.col("address").alias("address"),
        pl.col("channel").alias("channel"),
        pl.col("latitude").cast(pl.Float64).alias("latitude"),
        pl.col("longitude").cast(pl.Float64).alias("longitude"),
        pl.col("city").alias("city"),
        pl.col("secure_group_key"),
        pl.col("created_at_utc_datetime"),
        pl.col("created_at_date"),
        pl.col("data_source"),
        pl.col("data_provider_code"),
        # non standard columns into extra attrs
        pl.struct([pl.col(c) for c in non_standard_site_dim_cols]).alias(
            "extra_attributes"
        ),
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
def targetto_us_silver_week_fact(
    context: dg.AssetExecutionContext, targetto_us_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context, df=targetto_us_silver_day_fact, sampling_period="1w"
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
def targetto_us_silver_month_fact(
    context: dg.AssetExecutionContext, targetto_us_silver_day_fact: pl.LazyFrame
) -> pl.LazyFrame:
    df = silver_fact_downsample(
        context=context, df=targetto_us_silver_day_fact, sampling_period="1mo"
    )
    return df


defs = dg.Definitions(
    assets=[
        targetto_us_silver_day_fact,
        targetto_us_silver_prod_dim,
        targetto_us_silver_site_dim,
        targetto_us_silver_week_fact,
        targetto_us_silver_month_fact,
    ],
)
