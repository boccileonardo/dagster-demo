import dagster as dg
import polars as pl
from dagster_demo.components.gold import gold_store_fact_processing
from dagster_demo.defs.partitions import data_provider_partitions

from dagster_demo.components.polars_schemas import (
    store_fact_pl_schema,
    check_polars_schema,
)


@dg.asset(
    io_manager_key="gold_polars_parquet_io_manager",
    partitions_def=data_provider_partitions,
    metadata={
        "partition_by": "data_provider_code",
    },
    automation_condition=dg.AutomationCondition.eager(),
    group_name="refined",
    kinds={"polars", "deltalake", "gold"},
)
def gold_store_day_fact(
    context: dg.AssetExecutionContext,
    lidlo_de_silver_day_fact: pl.LazyFrame,
    carretwo_fr_silver_day_fact: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union silver assets and partition by retailer."""
    df = gold_store_fact_processing(
        context, [lidlo_de_silver_day_fact, carretwo_fr_silver_day_fact], "day"
    )
    return df


@dg.asset_check(asset=gold_store_day_fact, blocking=True)
def gold_store_day_fact_schema_check(
    context: dg.AssetCheckExecutionContext, gold_store_day_fact: pl.LazyFrame
):
    """Validate table schema:
    - Error if any expected columns are missing.
    - Error on wrong types.
    """
    check_results = check_polars_schema(
        df_schema=gold_store_day_fact.collect_schema(),
        expected_schema=store_fact_pl_schema,
        required_cols_list=store_fact_pl_schema.keys(),
    )
    return dg.AssetCheckResult(passed=check_results["passed"], metadata=check_results)


defs = dg.Definitions(
    assets=[gold_store_day_fact],
    asset_checks=[gold_store_day_fact_schema_check],
)
