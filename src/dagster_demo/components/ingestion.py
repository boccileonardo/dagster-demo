import polars as pl
import dagster as dg
import datetime as dt
from dagster_demo.components.logger import logger
from dagster_demo.components.output_metadata import add_materialization_metadata


def bronze_processing(context: dg.AssetExecutionContext, df: pl.LazyFrame, config):
    df = df.with_columns(secure_group_key=config.SECURE_GROUP_KEY)
    df = add_ingestion_metadata(
        df=df,
        data_source=config.DATA_SOURCE_NAME,
    )
    add_materialization_metadata(
        context=context, df=df, count_dates_in_col=config.DATE_COLUMN
    )
    return df


def add_ingestion_metadata(
    df: pl.LazyFrame,
    data_source: str,
):
    """Add ingestion details before saving a df into a landing zone."""
    ingestion_datetime = dt.datetime.now(tz=dt.timezone.utc)
    df = df.with_columns(
        ingested_at_utc_datetime=ingestion_datetime,
        ingested_at_date=ingestion_datetime.date(),  # allows partitioning by lower cardinality
        data_source=pl.lit(data_source),
    )
    logger.info(
        f"Added ingestion metadata to df. [{ingestion_datetime}, {data_source}]"
    )
    return df
