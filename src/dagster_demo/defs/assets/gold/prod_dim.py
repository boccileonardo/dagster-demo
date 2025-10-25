import dagster as dg
import polars as pl
from dagster_demo.components.gold import gold_prod_dim_processing
from dagster_demo.components.polars_schemas import (
    prod_dim_pl_schema,
    check_polars_schema,
)
from dagster_demo.defs.partitions import data_provider_partitions


@dg.asset(
    io_manager_key="gold_polars_delta_merge_io_manager",
    partitions_def=data_provider_partitions,
    metadata={
        "partition_expr": "data_provider_code",
        "merge_predicate": "s.prod_id = t.prod_id",
    },
    automation_condition=dg.AutomationCondition.eager(),
    group_name="refined",
    kinds={"polars", "deltalake", "gold"},
)
def gold_prod_dim(
    context: dg.AssetExecutionContext,
    lidlo_de_silver_prod_dim: pl.LazyFrame,
    carretwo_fr_silver_prod_dim: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union silver assets and partition by retailer."""
    df = gold_prod_dim_processing(
        context, [lidlo_de_silver_prod_dim, carretwo_fr_silver_prod_dim]
    )
    return df


@dg.asset_check(asset=gold_prod_dim, blocking=True)
def gold_prod_dim_schema_check(
    context: dg.AssetCheckExecutionContext, gold_prod_dim: pl.LazyFrame
):
    """Validate table schema:
    - Error if any expected columns are missing.
    - Error on wrong types.
    """
    check_results = check_polars_schema(
        df_schema=gold_prod_dim.collect_schema(),
        expected_schema=prod_dim_pl_schema,
        required_cols_list=prod_dim_pl_schema.keys(),
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


defs = dg.Definitions(
    assets=[gold_prod_dim],
    asset_checks=[gold_prod_dim_schema_check],
)
