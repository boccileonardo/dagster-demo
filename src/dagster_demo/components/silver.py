import polars as pl
import polars_hash as plh
import polars.selectors as cs
import dagster as dg
from typing import Literal
from datetime import datetime, timezone
from dagster_demo.components.logger import logger
from dagster_demo.components.output_metadata import add_materialization_metadata
from dagster_demo.components.bronze import add_ingestion_metadata
from dagster_demo.components.polars_schemas import (
    prod_dim_pl_schema,
    site_dim_pl_schema,
)
from dagster_demo.components.polars_schemas import (
    prod_dim_required_cols,
    site_dim_required_cols,
    store_fact_required_cols,
)


def _compute_scd_hash(
    df: pl.LazyFrame, scd_cols: list[str], key_prefix: str
) -> tuple[pl.LazyFrame, str]:
    """Compute xxh3_64 hash of SCD columns to create surrogate key.

    Automatically includes all columns with 'corp_' prefix to trigger SCD changes.
    """
    df_schema = df.collect_schema()
    schema_names = df_schema.names()

    key_col = f"{key_prefix}_key"

    # Automatically include all corporate master data columns
    corp_cols = [col for col in schema_names if col.startswith("corp_")]
    if corp_cols:
        logger.info(
            f"Auto-including {len(corp_cols)} corporate columns in SCD hash: {corp_cols}"
        )
    all_scd_cols = set(scd_cols + corp_cols)

    # Validate all SCD columns exist
    missing_cols = [col for col in all_scd_cols if col not in schema_names]
    if missing_cols:
        raise ValueError(
            f"SCD columns not found in dataframe: {missing_cols}. "
            f"Available columns: {schema_names}"
        )

    # Cast all columns to string and fill nulls for hash stability
    cast_exprs = [pl.col(col).cast(pl.String).fill_null("") for col in all_scd_cols]

    # Concatenate all SCD columns into single string, then hash
    # xxh3_64 returns UInt64 but Delta Lake/Arrow doesn't handle it
    # Use reinterpret() to change signedness without data loss
    df = (
        df.with_columns(pl.concat_str(cast_exprs, separator="|").alias("_scd_concat"))
        .with_columns(
            # fast hash function returns UInt64, reinterpret as Int64 for Delta Lake compatibility
            plh.col("_scd_concat")
            .nchash.xxh3_64()
            .reinterpret(signed=True)
            .alias(key_col)
        )
        .drop("_scd_concat")
    )

    return df, key_col


def _process_dimension_scd(
    context: dg.AssetExecutionContext,
    incoming_dim: pl.LazyFrame,
    existing_dim: pl.LazyFrame | None,
    scd_cols: list[str],
    natural_key: str,
    dim_table_name: str,
) -> pl.LazyFrame:
    """Process dimension table with SCD Type 2 logic.

    Detects changes in specified columns (`scd_cols`) via hashed surrogate kye.
    When a tracked attribute changes,
    the old version is marked as expired and a new version is created.
    Old versions are detected through a natural key column.
    """
    current_time = datetime.now(timezone.utc)

    # Compute hash surrogate keys for new data
    incoming_dim, surrogate_key_column = _compute_scd_hash(
        df=incoming_dim, scd_cols=scd_cols, key_prefix=dim_table_name
    )

    # Initial load: mark all rows as current
    if existing_dim is None:
        logger.info(f"Initial {dim_table_name} load - marking all rows as current")
        # For initial load, set valid_from to a date far in the past
        initial_valid_from = pl.lit(datetime(1900, 1, 1, tzinfo=timezone.utc))

        result = incoming_dim.with_columns(
            is_current=pl.lit(True),
            valid_from=initial_valid_from,
            valid_to=pl.lit(None, dtype=pl.Datetime(time_unit="us", time_zone="UTC")),
        ).unique(subset=[surrogate_key_column])

        add_materialization_metadata(
            context=context,
            df=result,
            count_rows=False,
            count_ids_in_col=surrogate_key_column,
        )
        return result

    # Find rows with new surrogate keys
    new_rows = incoming_dim.join(
        existing_dim.select([surrogate_key_column]), on=surrogate_key_column, how="anti"
    )

    # Stop early if no changes are detected
    if new_rows.limit(1).collect().is_empty():
        logger.info("No dimension changes detected - returning existing data")
        add_materialization_metadata(
            context=context,
            df=existing_dim,
            count_rows=False,
            count_ids_in_col=surrogate_key_column,
        )
        return existing_dim

    logger.info(
        f"Detected {new_rows.select(pl.len()).collect().item()} dimension changes"
    )

    # Identify which natural keys are being updated (vs brand new)
    current_versions = existing_dim.filter(pl.col("is_current") == True).select(  # noqa: E712
        [
            natural_key,
            pl.col(surrogate_key_column),
        ]
    )

    # Separate new rows into updates (existing natural key) vs truly new products
    updated_natural_keys = (
        new_rows.join(
            current_versions.select(natural_key).unique(), on=natural_key, how="inner"
        )
        .select(natural_key)
        .unique()
    )

    # Get the surrogate keys that need expiring
    updated_keys = (
        current_versions.join(updated_natural_keys, on=natural_key, how="inner")
        .select(surrogate_key_column)
        .unique()
    )

    # Expire old versions: set is_current=False and valid_to=now
    expired_rows = existing_dim.join(
        updated_keys, on=surrogate_key_column, how="inner"
    ).with_columns(
        is_current=pl.lit(False),
        valid_to=pl.lit(current_time),
    )

    # New products get far-past date, updated products get current time
    initial_valid_from = pl.lit(datetime(1900, 1, 1, tzinfo=timezone.utc))

    # Collect updated natural keys for the is_in check
    updated_nk_list = (
        updated_natural_keys.select(natural_key).collect().to_series().to_list()
    )

    new_versions = new_rows.with_columns(
        is_current=pl.lit(True),
        # If natural_key exists in current_versions, it's an update (use current_time)
        # Otherwise it's a new product (use far-past date)
        valid_from=pl.when(pl.col(natural_key).is_in(updated_nk_list))
        .then(pl.lit(current_time))
        .otherwise(initial_valid_from),
        valid_to=pl.lit(None, dtype=pl.Datetime(time_unit="us", time_zone="UTC")),
    ).unique(subset=[surrogate_key_column])

    # Return only changed rows: expired versions + new versions
    # IO manager will upsert these based on surrogate key merge predicate
    result = pl.concat([expired_rows, new_versions], how="diagonal_relaxed")

    add_materialization_metadata(
        context=context,
        df=result,
        count_rows=False,
        count_ids_in_col=surrogate_key_column,
    )

    return result


def _add_keys_to_fact(
    df: pl.LazyFrame,
    prod_dim: pl.LazyFrame,
    site_dim: pl.LazyFrame,
    fact_date_col: str = "time_period_end_date",
) -> pl.LazyFrame:
    """Join dimension surrogate keys to fact table using point-in-time logic.

    Matches fact records with the dimension version that was valid at the time
    of the transaction, based on the fact date falling within the dimension's
    validity period (valid_from <= fact_date < valid_to, or valid_to is null).

    Returns:
        Fact table enriched with prod_key and site_key columns
    """
    df_start_len = df.select(pl.len()).collect().item()
    # Join product dimension on natural key and date range
    df = (
        df.join(
            prod_dim.select(["prod_id", "prod_key", "valid_from", "valid_to"]),
            on="prod_id",
            how="inner",
        )
        .filter(
            (pl.col(fact_date_col) >= pl.col("valid_from"))
            & (
                (pl.col("valid_to").is_null())
                | (pl.col(fact_date_col) < pl.col("valid_to"))
            )
        )
        .drop(["valid_from", "valid_to"])
    )
    if df.select(pl.len()).collect().item() != df_start_len:
        raise ValueError(
            "Prod dim referential integrity error. Some fact rows are not present in dim."
        )

    # Join site dimension on natural key and date range
    df = (
        df.join(
            site_dim.select(["site_id", "site_key", "valid_from", "valid_to"]),
            on="site_id",
            how="inner",
        )
        .filter(
            (pl.col(fact_date_col) >= pl.col("valid_from"))
            & (
                (pl.col("valid_to").is_null())
                | (pl.col(fact_date_col) < pl.col("valid_to"))
            )
        )
        .drop(["valid_from", "valid_to"])
    )
    if df.select(pl.len()).collect().item() != df_start_len:
        raise ValueError(
            "Site dim referential integrity error. Some fact rows are not present in dim."
        )

    return df


def _prefix_cols(df: pl.LazyFrame, prefix: str, exclude: list[str] = []):
    """Add prefix to column names except for those in exclude list and required columns."""
    schema = df.collect_schema()
    col_names = schema.names()
    excluded_columns = set(
        prod_dim_required_cols
        + site_dim_required_cols
        + store_fact_required_cols
        + exclude
    )
    prefixed_col_names = [
        f"{prefix}_{col}" if col not in excluded_columns else col for col in col_names
    ]
    return df.rename(
        {col: new_col for col, new_col in zip(col_names, prefixed_col_names)}
    )


def _load_product_master_data():
    df = pl.scan_parquet("faker/data/corporate_product_master_data.parquet")
    return _prefix_cols(df, prefix="corp")


def _load_site_master_data():
    df = pl.scan_parquet("faker/data/corporate_site_master_data.parquet")
    return _prefix_cols(df, prefix="corp")


def column_name_is_in_data_model(table: str, col_name) -> bool:
    """Ensure the column name that is being mapped as non-standard is (not) in the data model.
    Guards against schema evolution (promoted fields from extra_attributes)."""
    match table:
        case "store_fact":
            return col_name in site_dim_pl_schema
        case "prod_dim":
            return col_name in prod_dim_pl_schema
        case "site_dim":
            return col_name in site_dim_pl_schema
        case _:
            # add table support to this function if hitting this error
            raise NotImplementedError("This table is not yet supported for validation.")


def silver_fact_processing(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    prod_dim: pl.LazyFrame,
    site_dim: pl.LazyFrame,
):
    df = df.unique()  # deduplication of bronze data

    # Apply global ID transformation: concatenate data_provider_code with natural keys
    df = df.with_columns(
        prod_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "prod_id"
        ),
        site_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "site_id"
        ),
    )

    # Add surrogate keys from dimension tables using point-in-time logic
    df = _add_keys_to_fact(
        df=df,
        prod_dim=prod_dim,
        site_dim=site_dim,
        fact_date_col="time_period_end_date",
    )

    add_materialization_metadata(
        context=context, df=df, count_dates_in_col="time_period_end_date"
    )
    return df


# TODO: figure out more elegant way to support local to cloud transition
def _load_existing_dimension(
    context: dg.AssetExecutionContext, dim_type: str
) -> pl.LazyFrame | None:
    """Load existing dimension table from Delta Lake storage.

    Uses the asset's IO manager configuration to determine the storage path.
    Returns None if the table doesn't exist yet (initial load).
    """
    # Build path where IO manager writes: root_uri/namespace/asset_name
    root_uri = "data/silver"
    asset_name = context.asset_key.path[-1]  # e.g., "carretwo_fr_silver_prod_dim"
    namespace = "public"  # Default namespace
    delta_path = f"{root_uri}/{namespace}/{asset_name}"

    try:
        # Check if Delta table exists by looking for _delta_log
        from pathlib import Path

        delta_log_path = Path(delta_path) / "_delta_log"
        if not delta_log_path.exists():
            logger.info(f"No {dim_type} dimension table at {delta_path} (initial load)")
            return None

        existing_dim = pl.scan_delta(delta_path)
        logger.info(f"Loaded existing {dim_type} dimension from {delta_path}")
        return existing_dim
    except Exception as e:
        logger.info(
            f"No existing {dim_type} dimension found at {delta_path} (initial load): {e}"
        )
        return None


def silver_prod_dim_processing(
    context: dg.AssetExecutionContext,
    incoming_dim: pl.LazyFrame,
    scd_cols: list[str],
):
    # Load existing dimension data for SCD processing
    existing_dim = _load_existing_dimension(context, dim_type="product")

    df = incoming_dim.unique(subset=["prod_id"])
    df = _prefix_cols(df, prefix="source", exclude=["extra_attributes"])

    if "source_item_gtin" in df.collect_schema().names():
        logger.info("Joining with corporate product master on GTIN.")
        master = _load_product_master_data()
        df = df.join(
            master,
            left_on="source_item_gtin",
            right_on="corp_item_gtin",
            how="left",
        )
    else:
        logger.info(
            "Skipping join with corporate master data due to missing GTIN for this retailer."
        )

    df = df.with_columns(
        prod_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "prod_id"
        )
    )

    # Apply SCD Type 2 processing
    df = _process_dimension_scd(
        context=context,
        incoming_dim=df,
        existing_dim=existing_dim,
        scd_cols=scd_cols,
        natural_key="prod_id",
        dim_table_name="prod",
    )

    return df


def silver_site_dim_processing(
    context: dg.AssetExecutionContext,
    incoming_dim: pl.LazyFrame,
    scd_cols: list[str],
):
    # Load existing dimension data for SCD processing
    existing_dim = _load_existing_dimension(context, dim_type="site")

    df = incoming_dim.unique(subset=["site_id"])
    df = _prefix_cols(df, prefix="source", exclude=["extra_attributes"])

    if "source_global_location_number" in df.collect_schema().names():
        master = _load_site_master_data()
        df = df.join(
            master,
            left_on="source_global_location_number",
            right_on="corp_global_location_number",
            how="left",
        )
    else:
        logger.warning(
            "Unable to join with site master data. global_location_number not found."
        )

    df = df.with_columns(
        site_id=pl.concat_str(
            pl.col("data_provider_code").str.strip_chars_start("cds_"), "site_id"
        )
    )

    # Apply SCD Type 2 processing
    df = _process_dimension_scd(
        context=context,
        incoming_dim=df,
        existing_dim=existing_dim,
        scd_cols=scd_cols,
        natural_key="site_id",
        dim_table_name="site",
    )

    return df


def silver_fact_downsample(
    context: dg.AssetExecutionContext,
    df: pl.LazyFrame,
    sampling_period: Literal["1w", "1mo"],
):
    """Return a dataframe with aggregate granularity.

    Args:
        df (pl.LazyFrame): more granular dataframe
        sampling_period ("1w" OR "1mo"): aggregation level for dates
    """
    df = df.sort("time_period_end_date")
    df = df.group_by_dynamic(
        index_column="time_period_end_date",
        group_by=["prod_id", "site_id", "data_provider_code"],
        every=sampling_period,
        label="right",
    ).agg(
        cs.numeric().sum(),
    )
    df = add_ingestion_metadata(df=df, data_source="aggregation")
    add_materialization_metadata(
        context=context,
        df=df,
        count_dates_in_col="time_period_end_date",
        count_rows=False,
    )
    return df
