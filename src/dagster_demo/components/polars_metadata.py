import polars as pl


def get_dataframe_metadata(df: pl.DataFrame | pl.LazyFrame):
    """"""
    if isinstance(df, pl.LazyFrame):
        row_count = df.select(pl.len()).collect().item()
        col_count = df.collect_schema().len()
    else:
        row_count, col_count = df.shape
    return {"rows": row_count, "columns": col_count}
