import polars as pl
import dagster as dg
import datetime as dt
from dagster_demo.components.logger import logger
from dagster_demo.components.constants import SECURE_GROUP_KEYS


def bronze_processing(
    context: dg.AssetExecutionContext, retailer_id: int, directory: str
):
    df = pl.scan_parquet(directory)
    # fail if no config in secure group key dict. Allow no SGK - treated as open access
    df = df.with_columns(secure_group_key=SECURE_GROUP_KEYS[retailer_id].get("sgk", 0))
    df = add_ingestion_metadata(
        context=context,
        df=df,
        data_source="uploader",
        source_file="carretwo",
        count_dates_in_col="date",
    )
    return df


def add_ingestion_metadata(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    data_source: str,
    source_file: str,
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
