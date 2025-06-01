import dagster_demo.defs

from dagster import definitions, load_defs


@definitions
def defs():
    return load_defs(defs_root=dagster_demo.defs)
