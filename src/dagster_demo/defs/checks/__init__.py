import dagster as dg


@dg.definitions
def asset_checks() -> dg.Definitions:
    return dg.Definitions(asset_checks={})
