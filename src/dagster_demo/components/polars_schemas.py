import polars as pl

prod_dim_pl_schema = {
    "prod_id": pl.String,
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date,
    "data_source": pl.String,
}


site_dim_pl_schema = {
    "site_id": pl.String,
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date,
    "data_source": pl.String,
}

store_fact_pl_schema = {
    "time_period_end_date": pl.Date,
    "prod_id": pl.String,
    "site_id": pl.String,
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date,
    "data_source": pl.String,
}


def check_polars_schema(df_schema: pl.Schema, expected_schema: dict):
    """Check whether a polars lazyframe's schema matches expectations.
    Passed is True if there are no mandatory column missing and no type mismatches against expectations.
    Return:{passed: bool, 'missing_cols': list, type_mismatches: list}
    """
    actual_names = set(df_schema.names())
    expected_names = set(expected_schema.keys())

    missing_cols = list(expected_names - actual_names)

    type_mismatches = []
    for col in expected_names & actual_names:
        actual_type = df_schema[col]
        expected_type = expected_schema[col]
        if expected_type != actual_type:
            type_mismatches.append(
                {
                    "column": col,
                    "expected": str(expected_type),
                    "actual": str(actual_type),
                }
            )
    result = not (missing_cols or type_mismatches)
    return {
        "passed": result,
        "missing_cols": sorted(missing_cols),
        "type_mismatches": type_mismatches,
    }
