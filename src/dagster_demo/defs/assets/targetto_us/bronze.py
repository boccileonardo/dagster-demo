import dagster as dg
import polars as pl
from dagster_demo.components.logger import logger
from dagster_demo.defs.assets.targetto_us import config as cfg
from dagster_demo.components.bronze import bronze_processing
from dagster_demo.components.sensors import detect_new_files_in_dir
from dagster_demo.defs.resources.freshness_policy import daily_policy


@dg.asset(
    io_manager_key="bronze_polars_delta_append_io_manager",
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "bronze"},
    freshness_policy=daily_policy,
)
def targetto_us_bronze_day_fact(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """targetto US one-big-table format for each store."""
    df = pl.scan_csv(cfg.DIRECTORY, separator="|")
    df = bronze_processing(
        context=context,
        df=df,
        config=cfg,
    )
    return df


job = dg.define_asset_job(
    name="bronze_targetto_us_job",
    selection=dg.AssetSelection.assets(targetto_us_bronze_day_fact),
)


@dg.sensor(
    minimum_interval_seconds=20,  # customize polling interval
    default_status=dg.DefaultSensorStatus.RUNNING,
    job=job,
)
def sensor_targetto_us_bronze_day_fact(context: dg.SensorEvaluationContext):
    new_files = detect_new_files_in_dir(directory=cfg.DIRECTORY, context=context)
    if new_files:
        logger.info(f"Found new files: {new_files}. Triggering run...")
        yield dg.RunRequest()
    else:
        yield dg.SkipReason("No new files found")


defs = dg.Definitions(
    assets=[targetto_us_bronze_day_fact],
    sensors=[sensor_targetto_us_bronze_day_fact],
)
