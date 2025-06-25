import polars as pl
import dagster as dg
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.polars_schemas import (
    store_fact_pl_schema,
    prod_dim_pl_schema,
    site_dim_pl_schema,
)


def gold_generic_processing(
    context: dg.AssetExecutionContext, base_df: pl.LazyFrame, assets: list[pl.LazyFrame]
):
    df = pl.concat(
        [
            base_df,
            *assets,
        ],
        how="diagonal_relaxed",
    )
    return df


def gold_prod_dim(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
) -> pl.LazyFrame:
    base_df = pl.LazyFrame(schema=prod_dim_pl_schema)
    return gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
    )


def gold_site_dim(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
) -> pl.LazyFrame:
    base_df = pl.LazyFrame(schema=site_dim_pl_schema)
    return gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
    )


def gold_store_fact(
    context: dg.AssetExecutionContext,
    assets: list[pl.LazyFrame],
) -> pl.LazyFrame:
    base_df = pl.LazyFrame(schema=store_fact_pl_schema)
    df = gold_generic_processing(
        context=context,
        base_df=base_df,
        assets=assets,
    )
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df
