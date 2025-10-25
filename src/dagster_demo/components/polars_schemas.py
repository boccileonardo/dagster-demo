import polars as pl
from typing import Iterable

prod_dim_pl_schema: dict[str, pl.DataType] = {
    # Required columns
    "prod_id": pl.Int32(),
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date(),
    "data_source": pl.String(),
    "data_provider_code": pl.String(),
    "secure_group_key": pl.Int32(),
    # Nullable columns
    "source_prod_name": pl.String(),
    "source_sector": pl.String(),
    "source_category": pl.String(),
    "source_subcategory": pl.String(),
    "source_item_gtin": pl.Int64(),
    "source_item_description": pl.String(),
    "source_prod_launch_date": pl.Date(),
    # Corporate product master data
    "corp_prod_name": pl.String(),
    "corp_sector": pl.String(),
    "corp_subcategory": pl.String(),
    "corp_category": pl.String(),
    "corp_item_description": pl.String(),
}

prod_dim_required_cols: list[str] = [
    "prod_id",
    "created_at_utc_datetime",
    "created_at_date",
    "data_source",
    "data_provider_code",
    "secure_group_key",
]

site_dim_pl_schema: dict[str, pl.DataType] = {
    # Required columns
    "site_id": pl.Int32(),
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date(),
    "data_source": pl.String(),
    "data_provider_code": pl.String(),
    "secure_group_key": pl.Int32(),
    # Nullable columns
    "global_location_number": pl.Int64(),
    "source_site_name": pl.String(),
    "source_address": pl.String(),
    "source_channel": pl.String(),
    "source_latitude": pl.Float64(),
    "source_longitude": pl.Float64(),
    "source_city": pl.String(),
    "source_state": pl.String(),
    "source_county": pl.String(),
    "source_country": pl.String(),
    # Corporate site master data
    "corp_site_name": pl.String(),
    "corp_address": pl.String(),
    "corp_channel": pl.String(),
    "corp_latitude": pl.Float64(),
    "corp_longitude": pl.Float64(),
    "corp_city": pl.String(),
    "corp_state": pl.String(),
    "corp_county": pl.String(),
    "corp_country": pl.String(),
}

site_dim_required_cols: list[str] = [
    "site_id",
    "created_at_utc_datetime",
    "created_at_date",
    "data_source",
    "data_provider_code",
    "secure_group_key",
]

store_fact_pl_schema: dict[str, pl.DataType] = {
    # Required columns
    "time_period_end_date": pl.Date(),
    "prod_id": pl.Int32(),
    "site_id": pl.Int32(),
    "created_at_utc_datetime": pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at_date": pl.Date(),
    "data_source": pl.String(),
    "data_provider_code": pl.String(),
    "secure_group_key": pl.Int32(),
    # Nullable columns
    "pos_sales_units": pl.Int64(),
    "pos_sales_value_usd": pl.Float64(),
    "pos_sales_value_lc": pl.Float64(),
    "returned_qty": pl.Int64(),
    "returned_value_usd": pl.Float64(),
    "returned_value_lc": pl.Float64(),
    "inventory_qty_on_hand": pl.Int64(),
    "inventory_qty_on_order": pl.Int64(),
    "inventory_value_on_hand_usd": pl.Float64(),
    "inventory_value_on_hand_lc": pl.Float64(),
}

store_fact_required_cols: list[str] = [
    "site_id",
    "prod_id",
    "time_period_end_date",
    "created_at_utc_datetime",
    "created_at_date",
    "data_source",
    "data_provider_code",
    "secure_group_key",
]


def check_polars_schema(
    df_schema: pl.Schema, expected_schema: dict, required_cols_list: Iterable[str]
):
    """Check whether a polars lazyframe's schema matches expectations.
    `Passed` = True if there are no mandatory column missing and no type mismatches against expectations and no unexpected columns.
    Return:{passed: bool, 'missing_cols': list, type_mismatches: list, 'unexpected_cols': list}
    """
    actual_names = set(df_schema.names())
    allowed_names = set(expected_schema.keys())

    # Find missing required columns
    missing_cols = [col for col in required_cols_list if col not in actual_names]

    # Find type mismatches
    type_mismatches = []
    # on intersection of sets to prevent keyerrors
    for col in allowed_names & actual_names:
        expected_type = expected_schema[col]
        actual_type = df_schema[col]
        if actual_type != expected_type:
            type_mismatches.append(
                {"col": col, "expected": str(expected_type), "actual": str(actual_type)}
            )

    # Find unexpected columns
    unexpected_cols = [col for col in actual_names if col not in allowed_names]

    # Passed if no missing required columns, no type mismatches, and no unexpected columns
    result = not missing_cols and not type_mismatches and not unexpected_cols

    return {
        "passed": result,
        "missing_cols": sorted(missing_cols),
        "type_mismatches": type_mismatches,
        "unexpected_cols": sorted(unexpected_cols),
    }
