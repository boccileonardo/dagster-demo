import polars as pl
import dagster as dg
from dagster_demo.components.logger import logger
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.polars_schemas import (
    store_fact_pl_schema,
    prod_dim_pl_schema,
    site_dim_pl_schema,
)


def gold_generic_processing(
    context: dg.AssetExecutionContext,
    base_df: pl.LazyFrame,
    assets: list[pl.LazyFrame],
    data_provider_num: int,
):
    logger.info(f"filtering to data provider: cds_{data_provider_num}")
    assets = [
        df.filter(pl.col("data_provider_code") == data_provider_num) for df in assets
    ]
    assets = [df for df in assets if not df.limit(1).collect().is_empty()]
    df = pl.concat(
        [
            base_df.filter(pl.col("data_provider_code") == f"cds_{data_provider_num}"),
            *assets,
        ],
        how="diagonal_relaxed",
    )
    context.log.info(f"len: {df.count().collect()}")
    return df


def gold_prod_dim_processing(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
) -> pl.LazyFrame:
    try:
        base_df = pl.scan_delta("data/gold/public/gold_prod_dim.delta")
    except Exception as e:
        logger.warning(f"encountered {e}, creating empty gold from known schema")
        base_df = pl.LazyFrame(schema=prod_dim_pl_schema)
    df = gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
        data_provider_num=context.partition_key,
    )
    add_materialization_metadata(context=context, df=df, count_rows=False)
    return df


def gold_site_dim_processing(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
) -> pl.LazyFrame:
    try:
        base_df = pl.scan_delta("data/gold/public/gold_site_dim.delta")
    except Exception as e:
        logger.warning(f"encountered {e}, creating empty gold from known schema")
        base_df = pl.LazyFrame(schema=site_dim_pl_schema)
    df = gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
        data_provider_num=context.partition_key,
    )
    add_materialization_metadata(context=context, df=df, count_rows=False)
    return df


def gold_store_fact_processing(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
    granularity: str,
) -> pl.LazyFrame:
    try:
        base_df = pl.scan_delta(f"data/gold/public/gold_store_{granularity}_fact.delta")
    except Exception as e:
        logger.warning(f"encountered {e}, creating empty gold from known schema")
        base_df = pl.LazyFrame(schema=store_fact_pl_schema)
    df = gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
        data_provider_num=context.partition_key,
    )
    add_materialization_metadata(
        context=context,
        df=df,
        count_dates_in_col="time_period_end_date",
        count_rows=False,
    )
    return df
