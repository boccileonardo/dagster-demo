# EXAMPLE DATA: faker/data/files_per_stroe

from dagster_demo.components.config_utils import build_retailer_config

RETAILER_ID = 1003
DATA_SOURCE_NAME = "delta-share"
DATE_COLUMN = "date"

DIRECTORY, SECURE_GROUP_KEY, RETAILER_NAME, REGION, COUNTRY = build_retailer_config(
    RETAILER_ID
)
