import dagster as dg
from dagster_delta import (
    DeltaLakePolarsIOManager,
    WriteMode,
    MergeConfig,
    MergeType,
    SchemaMode,
)
from dagster_delta.config import LocalConfig

defs = dg.Definitions(
    resources={
        "bronze_polars_delta_append_io_manager": DeltaLakePolarsIOManager(
            root_uri="data/bronze",
            mode=WriteMode.merge,
            schema_mode=SchemaMode.merge,
            merge_config=MergeConfig(
                merge_type=MergeType.deduplicate_insert,
            ),
            storage_options=LocalConfig(),
        ),  # append and be flexible with schema in bronze layer
        "silver_polars_delta_merge_io_manager": DeltaLakePolarsIOManager(
            root_uri="data/silver",
            mode=WriteMode.merge,
            merge_config=MergeConfig(
                merge_type=MergeType.upsert,
                source_alias="s",
                target_alias="t",
            ),
            storage_options=LocalConfig(),
        ),  # requires passing merge predicate: metadata={"merge_predicate": "s.foo = t.foo AND s.bar = t.bar"},
        "gold_polars_delta_merge_io_manager": DeltaLakePolarsIOManager(
            root_uri="data/gold",
            mode=WriteMode.merge,
            merge_config=MergeConfig(
                merge_type=MergeType.upsert,
                source_alias="s",
                target_alias="t",
            ),
            storage_options=LocalConfig(),
        ),
    }
)
