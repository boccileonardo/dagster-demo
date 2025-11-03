import dagster as dg
import polars as pl


def add_materialization_metadata(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    count_dates_in_col: str = "",
    count_ids_in_col: str = "",
    count_rows: bool = True,
):
    if count_dates_in_col:
        num_dates = df.select(count_dates_in_col).unique().count().collect().item()
        context.add_output_metadata({"Dates": dg.MetadataValue.int(num_dates)})
    if count_ids_in_col:
        num_ids = df.select(count_ids_in_col).unique().count().collect().item()
        context.add_output_metadata({"Unique IDs": dg.MetadataValue.int(num_ids)})
    if count_rows:
        num_rows = df.select(pl.len()).collect().item()
        context.add_output_metadata({"Rows": dg.MetadataValue.int(num_rows)})
