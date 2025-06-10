import dagster as dg
from dagster_polars import PolarsDeltaIOManager

defs = dg.Definitions(
    resources={
        "bronze_polars_parquet_io_manager": PolarsDeltaIOManager(
            base_dir="data/bronze", mode="append"
        ),
        "silver_polars_parquet_io_manager": PolarsDeltaIOManager(
            base_dir="data/silver", mode="overwrite"
        ),
    },
)
