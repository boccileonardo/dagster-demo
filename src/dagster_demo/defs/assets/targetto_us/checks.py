import dagster as dg
import polars as pl
from dagster_demo.components.polars_schemas import (
    store_fact_pl_schema,
    store_fact_required_cols,
    site_dim_pl_schema,
    site_dim_required_cols,
    prod_dim_pl_schema,
    prod_dim_required_cols,
    check_polars_schema,
)
from dagster_demo.defs.assets.targetto_us.silver import (
    targetto_us_silver_prod_dim,
    targetto_us_silver_site_dim,
    targetto_us_silver_day_fact,
    targetto_us_silver_week_fact,
    targetto_us_silver_month_fact,
)


@dg.asset_check(asset=targetto_us_silver_prod_dim, blocking=True)
def silver_prod_dim_schema_check(
    context: dg.AssetCheckExecutionContext, targetto_us_silver_prod_dim: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=targetto_us_silver_prod_dim.collect_schema(),
        expected_schema=prod_dim_pl_schema,
        required_cols_list=prod_dim_required_cols,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=targetto_us_silver_site_dim, blocking=True)
def silver_site_dim_schema_check(
    context: dg.AssetCheckExecutionContext, targetto_us_silver_site_dim: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=targetto_us_silver_site_dim.collect_schema(),
        expected_schema=site_dim_pl_schema,
        required_cols_list=site_dim_required_cols,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=targetto_us_silver_day_fact, blocking=True)
def silver_day_fact_schema_check(
    context: dg.AssetCheckExecutionContext, targetto_us_silver_day_fact: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=targetto_us_silver_day_fact.collect_schema(),
        expected_schema=store_fact_pl_schema,
        required_cols_list=store_fact_required_cols,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=targetto_us_silver_week_fact, blocking=True)
def silver_week_fact_schema_check(
    context: dg.AssetCheckExecutionContext, targetto_us_silver_week_fact: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=targetto_us_silver_week_fact.collect_schema(),
        expected_schema=store_fact_pl_schema,
        required_cols_list=store_fact_required_cols,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=targetto_us_silver_month_fact, blocking=True)
def silver_month_fact_schema_check(
    context: dg.AssetCheckExecutionContext, targetto_us_silver_month_fact: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=targetto_us_silver_month_fact.collect_schema(),
        expected_schema=store_fact_pl_schema,
        required_cols_list=store_fact_required_cols,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)
