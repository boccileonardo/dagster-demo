# EXAMPLE DATA: faker/data/daily_files

from dagster_demo.components.config_utils import build_retailer_config

RETAILER_ID = 1001
DATA_SOURCE_NAME = "uploader"
DATE_COLUMN = "date"

DIRECTORY, SECURE_GROUP_KEY, RETAILER_NAME, REGION, COUNTRY = build_retailer_config(
    RETAILER_ID
)

# SCD Type 2 tracked columns - columns that trigger dimension versioning when changed
PROD_DIM_SCD_COLS = [
    "source_prod_name",
]

SITE_DIM_SCD_COLS = [
    "source_site_name",
]
