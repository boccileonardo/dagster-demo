import dagster as dg
import polars as pl
from dagster_demo.components.polars_schemas import (
    store_fact_pl_schema,
    site_dim_pl_schema,
    prod_dim_pl_schema,
    check_polars_schema,
)
from dagster_demo.defs.assets.lidlo_de.silver import (
    lidlo_de_silver_prod_dim,
    lidlo_de_silver_site_dim,
    lidlo_de_silver_day_fct,
    lidlo_de_silver_week_fct,
    lidlo_de_silver_month_fct,
)


@dg.asset_check(asset=lidlo_de_silver_prod_dim, blocking=True)
def silver_prod_dim_schema_check(
    context: dg.AssetCheckExecutionContext, lidlo_de_silver_prod_dim: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=lidlo_de_silver_prod_dim.collect_schema(),
        expected_schema=prod_dim_pl_schema,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=lidlo_de_silver_site_dim, blocking=True)
def silver_site_dim_schema_check(
    context: dg.AssetCheckExecutionContext, lidlo_de_silver_site_dim: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=lidlo_de_silver_site_dim.collect_schema(),
        expected_schema=site_dim_pl_schema,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=lidlo_de_silver_day_fct, blocking=True)
def silver_day_fct_schema_check(
    context: dg.AssetCheckExecutionContext, lidlo_de_silver_day_fct: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=lidlo_de_silver_day_fct.collect_schema(),
        expected_schema=store_fact_pl_schema,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=lidlo_de_silver_week_fct, blocking=True)
def silver_week_fct_schema_check(
    context: dg.AssetCheckExecutionContext, lidlo_de_silver_week_fct: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=lidlo_de_silver_week_fct.collect_schema(),
        expected_schema=store_fact_pl_schema,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


@dg.asset_check(asset=lidlo_de_silver_month_fct, blocking=True)
def silver_month_fct_schema_check(
    context: dg.AssetCheckExecutionContext, lidlo_de_silver_month_fct: pl.LazyFrame
):
    """Validate table schema:
    - Error if primary keys missing.
    - Error on wrong types.

    """
    check_results = check_polars_schema(
        df_schema=lidlo_de_silver_month_fct.collect_schema(),
        expected_schema=store_fact_pl_schema,
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)
