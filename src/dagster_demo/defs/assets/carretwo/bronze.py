import dagster as dg
import polars as pl
from dagster_polars import PolarsDeltaIOManager
from dagster_demo.components.constants import LANDING_ZONE
from dagster_demo.components.ingestion import bronze_processing
from dagster_demo.components.sensors import detect_new_files_in_dir

RETAILER_ID = 1001
DIRECTORY = f"{LANDING_ZONE}/{RETAILER_ID}/"


@dg.asset(io_manager_key="bronze_polars_parquet_io_manager", group_name="carretwo")
def bronze_carretwo_day_fct(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    df = bronze_processing(
        context=context,
        retailer_id=RETAILER_ID,
        directory=DIRECTORY,
    )
    return df


@dg.sensor(
    minimum_interval_seconds=30,  # customize polling interval
    default_status=dg.DefaultSensorStatus.RUNNING,
    asset_selection=f'key:"{bronze_carretwo_day_fct.key}"',
)
def sensor_bronze_carretwo_day_fct(context: dg.SensorEvaluationContext):
    new_files = detect_new_files_in_dir(directory=DIRECTORY, context=context)
    if new_files:
        for filename in new_files:
            yield dg.RunRequest(run_key=str(filename))
    else:
        yield dg.SkipReason("No new files found")


defs = dg.Definitions(
    assets=[bronze_carretwo_day_fct],
    resources={
        "bronze_polars_parquet_io_manager": PolarsDeltaIOManager(
            base_dir="data/bronze", mode="append"
        )
    },
    sensors=[sensor_bronze_carretwo_day_fct],
)
