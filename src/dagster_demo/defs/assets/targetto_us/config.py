# EXAMPLE DATA: faker/data/files_per_stroe

from dagster_demo.components.config_utils import build_retailer_config

RETAILER_ID = 1003
DATA_SOURCE_NAME = "delta-share"
DATE_COLUMN = "date"

DIRECTORY, SECURE_GROUP_KEY, RETAILER_NAME, REGION, COUNTRY = build_retailer_config(
    RETAILER_ID
)

# SCD Type 2 tracked columns - columns that trigger dimension versioning when changed
PROD_DIM_SCD_COLS = [
    "source_prod_name",
    "source_sector",
    "source_category",
    "source_item_gtin",
    "source_item_description",
    "source_prod_launch_date",
]

SITE_DIM_SCD_COLS = [
    "source_global_location_number",
    "source_site_name",
    "source_address",
    "source_channel",
    "source_latitude",
    "source_longitude",
    "source_city",
]
