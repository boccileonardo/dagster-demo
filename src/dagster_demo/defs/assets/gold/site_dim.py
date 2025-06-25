import dagster as dg
import polars as pl
from dagster_demo.components.gold import gold_site_dim


@dg.asset(
    io_manager_key="gold_polars_parquet_io_manager",
    automation_condition=dg.AutomationCondition.eager(),
    group_name="refined",
    kinds={"polars", "deltalake", "gold"},
)
def gold_store_site_dim(
    context: dg.AssetExecutionContext,
    lidlo_de_silver_site_dim: pl.LazyFrame,
    carretwo_fr_silver_site_dim: pl.LazyFrame,
) -> pl.LazyFrame:
    """Union silver assets and partition by retailer."""
    df = gold_site_dim(context, [lidlo_de_silver_site_dim, carretwo_fr_silver_site_dim])
    return df


defs = dg.Definitions(
    assets=[gold_store_site_dim],
)
