import polars as pl
import datetime as dt
from dagster_demo.components.logger import logger


def add_ingestion_metadata(
    df: pl.DataFrame | pl.LazyFrame, data_source: str, source_file: str
):
    """Add ingestion details before saving a df into a landing zone."""
    ingestion_datetime = dt.datetime.now(tz=dt.timezone.utc)
    df = df.with_columns(
        ingested_at_utc_datetime=ingestion_datetime,
        ingested_at_date=ingestion_datetime.date,  # allows partitioning by lower cardinality
        data_source=data_source,
        source_file=source_file,
    )
    logger.info(
        f"Added ingestion metadata to df. [{ingestion_datetime}, {data_source}, {source_file}]"
    )
    return df
