# EXAMPLE DATA: faker/data/single_file_many_dates

from dagster_demo.components.config_utils import build_retailer_config

RETAILER_ID = 1002
DATA_SOURCE_NAME = "uploader"
DATE_COLUMN = "date"

DIRECTORY, SECURE_GROUP_KEY, RETAILER_NAME, REGION, COUNTRY = build_retailer_config(
    RETAILER_ID
)

# SCD Type 2 tracked columns - columns that trigger dimension versioning when changed
PROD_DIM_SCD_COLS = [
    "source_prod_name",
    "source_category",
    "source_sector",
    "source_item_gtin",
    "source_item_description",
    "source_prod_launch_date",
]

SITE_DIM_SCD_COLS = [
    "source_site_name",
    "source_city",
    "source_address",
    "source_channel",
    "source_latitude",
    "source_longitude",
    "source_global_location_number",
]
