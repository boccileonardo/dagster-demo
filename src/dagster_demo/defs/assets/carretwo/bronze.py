import dagster as dg
import polars as pl
from dagster_demo.components.logger import logger
from dagster_demo.components.constants import LANDING_ZONE
from dagster_demo.components.ingestion import add_ingestion_metadata

RETAILER_ID = '1001'

@dg.asset(io_manager_key="polars_parquet_io_manager")
def bronze_carretwo_day_fct(context: dg.AssetExecutionContext):
    df = pl.scan_parquet(f'{LANDING_ZONE}/{RETAILER_ID}/')
    df = add_ingestion_metadata(df, data_source='uploader', source_file='')

materialization_job = dg.define_asset_job("bronze_materialization_job", selection=[bronze_carretwo_day_fct])

