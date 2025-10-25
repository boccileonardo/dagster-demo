import dagster as dg
from dagster_demo.components.constants import RETAILER_CONFIG

_data_providers_list = [f"cds_{key}" for key in RETAILER_CONFIG.keys()]
data_provider_partitions = dg.StaticPartitionsDefinition(_data_providers_list)
