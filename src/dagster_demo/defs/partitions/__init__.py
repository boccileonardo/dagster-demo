import dagster as dg
from dagster_demo.components.constants import RETAILER_CONFIG

data_providers_str_list = [str(key) for key in RETAILER_CONFIG.keys()]
data_provider_partitions = dg.StaticPartitionsDefinition(data_providers_str_list)
