# Data Model - Dimension SCD and surrogate keys

The product and site dimension tables use Slowly Changing Dimension (SCD) Type 2 to retain historical values of the same product or store/warehouse. This enables point-in-time analytics, allowing comparison of various product or site versions that have changed in key attributes, without being assigned a new identifier. The process is idempotent and ensures data integrity with appropriate data quality checks before executing writes to silver tables.

Natural keys are stored in both fact and dimension tables, enabling two query access patterns:
    A. Use the surrogate key to join with dimension tables, accessing the historical version of a product or site.
    B. Use the natural key with a `WHERE` clause filtering `is_current=TRUE` to access only the current values in the dimension table.

The hashing algorithm uses the `xxh3_64` function (provided by the `polars-hash` library) to hash a set of slowly changing attribute columns. These columns are configured per-retailer, and the implementation automatically includes all columns with the `corp_` prefix (corporate master data) in the hash computation, ensuring that changes in corporate master data trigger SCD versioning.

## Dimension table processing

When a change in one of the tracked columns is detected (resulting in a different hash), the following process executes:

1. **Hash stability**: All tracked attribute columns are cast to strings and null values are filled with empty strings to ensure hash stability.

2. **Hash computation**: The configured SCD columns plus all `corp_` prefixed columns are concatenated and hashed using `xxh3_64`, producing an Int64 surrogate key (e.g., `prod_key` or `site_key`).

3. **Identify new versions**: An anti-join on the surrogate key against the existing dimension table identifies rows with new hash values (changed or new entities).

4. **Early exit optimization**: If no new hash values are detected, the process returns the existing dimension data immediately without further processing.

5. **Distinguish updates from new entities**: Join the identified rows with the existing dimension table on the natural key (filtering `is_current=TRUE`) to separate:
   - Updates: natural key exists in current dimension (entity has changed)
   - New entities: natural key doesn't exist (brand new product/site)

6. **Expire old versions**: For updates, retrieve the old surrogate keys and prepare expired rows by setting `is_current=False` and `valid_to=current_datetime`.

7. **Create new versions**: Prepare new rows with:
   - `is_current=True`
   - `valid_from=current_datetime` for updates
   - `valid_from=1900-01-01` for new entities (backdated to cover all historical fact records)
   - `valid_to=null`
   - Deduplicated on surrogate key

8. **Single upsert write**: Combine expired rows and new rows, then write to Delta Lake. The IO manager performs a single upsert operation using the surrogate key as the merge predicate (e.g., `s.prod_key = t.prod_key`), which handles both expiration updates and new row insertions atomically.

## Fact table processing

Silver fact tables depend on dimension tables to ensure surrogate key and SCD processing is complete before fact processing begins.

1. **Asset dependencies**: Silver dimension tables are declared as dependencies for silver fact Dagster assets.

2. **Point-in-time surrogate key join**: Fresh batches of fact data are joined with silver dimensions on:
   - Natural key match (`prod_id` and `site_id`)
   - Temporal validity: `fact_date >= valid_from AND (valid_to IS NULL OR fact_date < valid_to)`

   This ensures each fact record references the dimension version that was valid at the time of the transaction.

3. **Surrogate key enrichment**: The join adds surrogate key columns (`prod_key` and `site_key`) to each fact row.

4. **Referential integrity validation**: After each dimension join, row counts are validated to ensure no fact records are lost, raising an error if any facts lack corresponding dimension records.

5. **Write to Delta Lake**: The silver fact table is written containing both surrogate keys (for historical versioning) and natural keys (for current-state queries), enabling multiple query access patterns. Facts are upserted using a composite merge predicate on natural keys and the date column (e.g., `s.time_period_end_date = t.time_period_end_date AND s.prod_id = t.prod_id AND s.site_id = t.site_id`).
