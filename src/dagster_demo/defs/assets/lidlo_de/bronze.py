import dagster as dg
import polars as pl
from dagster_demo.components.logger import logger
from dagster_demo.defs.assets.lidlo_de import config as cfg
from dagster_demo.components.bronze import bronze_processing
from dagster_demo.components.sensors import detect_new_files_in_dir


@dg.asset(
    io_manager_key="bronze_polars_parquet_io_manager",
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
    },
    kinds={"polars", "deltalake", "bronze"},
)  # type: ignore[call-overload]
def lidlo_de_bronze_day_fct(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    df = pl.scan_parquet(cfg.DIRECTORY)
    df = bronze_processing(
        context=context,
        df=df,
        config=cfg,
    )
    return df


job = dg.define_asset_job(
    name="bronze_lidlo_de_job",
    selection=dg.AssetSelection.assets(lidlo_de_bronze_day_fct),
)


@dg.sensor(
    minimum_interval_seconds=20,  # customize polling interval
    default_status=dg.DefaultSensorStatus.RUNNING,
    job=job,
)
def sensor_lidlo_de_bronze_day_fct(context: dg.SensorEvaluationContext):
    new_files = detect_new_files_in_dir(directory=cfg.DIRECTORY, context=context)
    if new_files:
        logger.info(f"Found new files: {new_files}. Triggering run...")
        yield dg.RunRequest()
    else:
        yield dg.SkipReason("No new files found")


defs = dg.Definitions(
    assets=[lidlo_de_bronze_day_fct],
    sensors=[sensor_lidlo_de_bronze_day_fct],
)
