import dagster as dg
import polars as pl
from dagster_demo.components.gold import gold_store_fact


@dg.asset(
    io_manager_key="gold_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name="refined",
    kinds={"polars", "deltalake", "gold"},
)
def gold_store_week_fct(
    context: dg.AssetExecutionContext,
    lidlo_de_silver_week_fct: pl.LazyFrame,
    carretwo_fr_silver_week_fct: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union silver assets and partition by retailer."""
    df = gold_store_fact(
        context, [lidlo_de_silver_week_fct, carretwo_fr_silver_week_fct]
    )
    return df


defs = dg.Definitions(
    assets=[gold_store_week_fct],
)
