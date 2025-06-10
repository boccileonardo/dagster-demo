import polars as pl
import dagster as dg
import datetime as dt
from dagster_demo.components.logger import logger


def add_ingestion_metadata(
    df: pl.DataFrame | pl.LazyFrame,
    data_source: str,
    source_file: str,
    context: dg.AssetExecutionContext,
    count_dates_in_col: str = "",
    count_rows: bool = True,
):
    """Add ingestion details before saving a df into a landing zone."""
    ingestion_datetime = dt.datetime.now(tz=dt.timezone.utc)
    df = df.with_columns(
        ingested_at_utc_datetime=ingestion_datetime,
        ingested_at_date=ingestion_datetime.date(),  # allows partitioning by lower cardinality
        data_source=pl.lit(data_source),
        source_file=pl.lit(source_file),
    )
    logger.info(
        f"Added ingestion metadata to df. [{ingestion_datetime}, {data_source}, {source_file}]"
    )
    if count_dates_in_col:
        num_dates = df.select(count_dates_in_col).unique().count().collect().item()
        context.add_output_metadata({"Dates": dg.MetadataValue.int(num_dates)})
    if count_rows:
        num_rows = df.select(pl.len()).collect().item()
        context.add_output_metadata({"Rows": dg.MetadataValue.int(num_rows)})
    return df
