# Dagster Demo

Event-driven ETL of retailer point-of-sale data through Polars (small data), Pyspark on Databricks (big data) into Delta lake tables.

### Design approach:
- [ ] Gold views: Apply user-specific row-level security, antitrust masking logic to gold tables.
- [ ] Gold tables: Single unity catalog table per object type (eg. site_dim, prod_dim, daily_store_fct), partitioned by retailer code, recorded in metadata for discoverability, unioning all retailer specific tables.
- [x] Silver tables: Retailer-level tables (natively received, as well as derived from aggregation), deduplicated, sanitized by DQ checks, and enriched by reference/master data from corporate sources.
- [x] Bronze tables: Retailer-level tables, append-only raw data, sensor-based triggers where possible, timestamped with ingestion time, with assigned secure group key. Date partitioning only required if big data.

- Retailer-level access is controlled by secure group keys via Unity catalog dynamic views.
- Antitrust masking (only for competition data) is applied via dynamic views too, with a CASE .. WHEN logic that selects either the masked, or the real column depending on record age.

#TODO: Use a data quality library to run dq checks in silver
#TODO: think about partition inheritance at gold layer from previously partitioned assets

Demo should include retailers that:
- Send data via web UI/SFTP/Sharepoint to landing zone.
- Send data via cloud warehouse/direct DB connection.
- Send data via API.
- Send one-big-table with product, site, sales and inventory.
- Send separate dimension and fact tables.
- Send multiple files for each date.
- Send a single file with many dates.
- Send multiple files for each store, each including multiple dates.
- Send daily files.
- Send weekly files containing a single week-end date.
- Send weekly files containing all days of the previous week.
