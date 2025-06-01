import dagster as dg


@dg.asset
def my_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
