import os
import dagster as dg
import polars as pl
from datetime import datetime
from dagster_demo.defs.assets.targetto_us import config as cfg
from dagster_demo.components.bronze import bronze_processing
from dagster_demo.components.sensors import (
    detect_new_files_in_dir,
    process_new_partitions_in_files,
)
from dagster_demo.defs.resources.freshness_policy import daily_policy


@dg.asset(
    io_manager_key="bronze_polars_delta_append_io_manager",
    group_name=cfg.RETAILER_NAME,
    metadata={
        "ssid": cfg.RETAILER_ID,
        "name": cfg.RETAILER_NAME,
        "region": cfg.REGION,
        "country": cfg.COUNTRY,
        "partition_expr": "date",
    },
    kinds={"polars", "deltalake", "bronze"},
    partitions_def=dg.DailyPartitionsDefinition(start_date=datetime(2025, 8, 8)),
    freshness_policy=daily_policy,
)
def targetto_us_bronze_day_fact(context: dg.AssetExecutionContext) -> pl.LazyFrame:
    """targetto US one-big-table format for each store."""
    df = pl.scan_csv(cfg.DIRECTORY, separator="|")
    if context.has_partition_key:  # partitioned runs
        df = df.filter(pl.col("date") == context.partition_key)
        if df.limit(1).collect().is_empty():
            raise ValueError(
                f"No data available for partition: {context.partition_key}"
            )
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
    """
    Sensor that detects new files and launches partitioned runs for each unique date.
    Each partition (date) found in new files triggers a separate RunRequest.
    """
    # Detect which files are new
    new_file_paths = detect_new_files_in_dir(context=context, directory=cfg.DIRECTORY)

    if not new_file_paths:
        yield dg.SkipReason("No new files found")
        return

    context.log.info(
        f"Found {len(new_file_paths)} new files: {[os.path.basename(f) for f in new_file_paths]}"
    )

    # Parse the new files and extract unique date partitions
    df = pl.scan_csv(new_file_paths, separator="|")
    unique_dates = df.select(pl.col("date")).unique().collect()["date"].to_list()

    context.log.info(
        f"Found {len(unique_dates)} unique date partitions: {unique_dates}"
    )

    # Generate RunRequests for each partition
    run_requests = process_new_partitions_in_files(
        partition_keys=unique_dates, asset_name="targetto_us_bronze"
    )

    for run_request in run_requests:
        yield run_request


defs = dg.Definitions(
    assets=[targetto_us_bronze_day_fact],
    sensors=[sensor_targetto_us_bronze_day_fact],
)
