# Retailer Onboarding

To onboard a new retailer, follow these steps:

1. Add the appropriate directory: `src/dagster_demo/defs/assets/{retailer_countrycode}/`
2. Replicate file structure from another retailer directory and appropriately rename assets and update docstrings
3. Add the new retailer to the config files: `src/dagster_demo/defs/assets/{retailer_countrycode}/` and `src/dagster_demo/components/constants.py`.
4. Perform the column mapping in `src/dagster_demo/defs/assets/{retailer_countrycode}/silver.py`. Non-standard columns (not found in `src/dagster_demo/components/polars_schemas.py`) should be mapped to the struct column `extra_attributes`.
5. Update the dependency graph by adding the newly created *silver* assets as dependencies to the *gold* assets (`src/dagster_demo/defs/assets/gold`).
