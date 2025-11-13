# Dagster Demo

Event-driven ETL of retailer point-of-sale data through Polars (small data), Pyspark on Databricks (big data) into Delta lake tables.

### Quick Start
```sh
cd ~
mkdir repos && cd repos
git clone https://github.com/boccileonardo/dagster-demo.git
cd dagster-demo
mkdir .dagster_home
echo 'export DAGSTER_HOME="~/repos/dagster-demo/.dagster_home"' >> ~/.bashrc
```
Restart shell
```sh
uv run faker/generate.py
./setup-data.sh
# turn on the eager automation sensor in the dagster UI to allow auto materialization
uv sync
uv run dg dev
```

### Design approach:
- Gold views: Single unity catalog table per object type (eg. site_dim, prod_dim, daily_store_fact), with dynamic row-level security, antitrust masking logic to gold tables, recorded in metadata for discoverability, unioning all retailer specific tables.
- Silver tables: Retailer-level tables (natively received, as well as derived from aggregation like `daily`->`weekly`), deduplicated, sanitized by DQ checks, and enriched by reference/master data from corporate sources, contain both masked antitrust and original columns.
- Bronze tables: Retailer-level tables, append-only, raw data with merge schema mode, sensor-based triggers where possible, timestamped with ingestion time, with assigned secure group key and data provider code. Date partitioning only required if big data.

- Surrogate keys in SCD2 are the result of hashing of natural keys.
- Retailer-level row-level-security in shared gold views is controlled by secure group keys.
- Antitrust masking (only for competition data) is applied via column selection logic that selects either the masked, or the real column depending on record age.

Demo includes retailers that:
- Send data via web UI/SFTP/Sharepoint to landing zone.
- Send data via cloud warehouse/direct DB connection.
- Send data via API.
