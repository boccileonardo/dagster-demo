# AGENTS.md

## Project Overview

This repository implements a medallion architecture data lakehouse for retailer point-of-sale data, orchestrated with Dagster. Data flows through Bronze (raw), Silver (cleaned/enriched), and Gold (unified/secure) layers, using Polars for data processing and Delta Lake for storage.

## Key Technologies

- **Dagster**: Assets and checks are defined in Python using Dagster's software defined assets, not legacy dagster patterns such as ops.
- **Polars**: Lazyframes are used, with collect() calls limited where absolutely necessary.
- **Delta Lake**: Storage format.
- **dagster-delta**: Provides IOManagers for reading/writing Polars LazyFrames to Delta Lake and supporting merge statements.
- **uv**: Package and environment manager. Only allowed to use uv to run terminal commands to execute python.

## Medallion Architecture

- **Bronze Layer**: Raw, append-only retailer data. Uses `bronze_polars_delta_append_io_manager` (schema mode: merge, write mode: append). Sensor-based triggers ingest files from landing zones.
- **Silver Layer**: Cleaned, deduplicated, and enriched tables. Uses `silver_polars_delta_merge_io_manager` (write mode: merge, upsert on natural keys). Surrogate keys (SCD2) are generated using `xxh3_64` hash of configured columns plus all `corp_` prefixed columns.
- **Gold Layer**: Unified, retailer-agnostic views. Uses `gold_polars_delta_merge_io_manager` (write mode: merge, upsert on composite keys). Implements row-level security and antitrust masking logic.

## Data Model

See `dimensional-model.md` for details on SCD2 dimension processing and surrogate key logic. Surrogate keys are generated for product and site dimensions, enabling point-in-time analytics and referential integrity in fact tables.

## Asset Definitions

- Assets (bronze and silver) are defined per retailer in `src/dagster_demo/defs/assets/{retailer_countrycode}/`.
- Gold assets union all retailer silver tables, partitioned by `data_provider_code`.
- Asset checks validate schema and data quality.

## Onboarding New Retailers

See `retailer-onboarding.md` for step-by-step instructions.

## Development & Usage

- Use `uv` for running scripts and managing dependencies.
- Main entrypoints:
  - `uv run faker/generate.py` (generate synthetic data)
  - `sudo ./setup-data.sh` (setup data directories and clean up)
  - `uv run dg dev` (start Dagster development server)
  - `uv run dg launch --assets {assetname}` (optionally add `--partition {partitionkey}` for partitioned assets.)

## Directory Structure

- `assets/`: Asset definitions (bronze, silver, gold) per retailer.
- `components/`: Shared ETL logic (bronze, silver, gold processing).
- `data/`: Local dev storage.
