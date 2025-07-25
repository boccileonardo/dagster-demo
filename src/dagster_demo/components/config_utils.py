from dagster_demo.components.constants import LANDING_ZONE, RETAILER_CONFIG


def build_retailer_config(RETAILER_ID: int):
    DIRECTORY = f"{LANDING_ZONE}/{RETAILER_ID}/"
    # fail if no config in secure group key dict. Allow no SGK - treated as open access
    SECURE_GROUP_KEY = RETAILER_CONFIG[RETAILER_ID].get("sgk", 0)
    RETAILER_NAME = RETAILER_CONFIG[RETAILER_ID]["name"]
    REGION = RETAILER_CONFIG[RETAILER_ID]["region"]
    COUNTRY = RETAILER_CONFIG[RETAILER_ID]["country"]
    return DIRECTORY, SECURE_GROUP_KEY, RETAILER_NAME, REGION, COUNTRY
