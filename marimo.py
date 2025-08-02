import marimo

__generated_with = "0.14.16"
app = marimo.App()


@app.cell
def _():
    old_data = {"id": [1, 2, 3, 4], "txt": ["oldA", "b", "c", "d"]}
    new_data = {"id": [1, 2, 3], "txt": ["newA", "b", "c"]}
    return new_data, old_data


@app.cell
def _(new_data, old_data):
    import polars as pl

    _writepath = "test_partitioned_write_only.delta"
    pl.DataFrame(old_data).write_delta(
        _writepath, delta_write_options={"partition_by": "id"}
    )
    _merge_predicate = "\ns.id = t.id\n"
    table_merger = pl.DataFrame(new_data).write_delta(
        _writepath,
        mode="merge",
        delta_merge_options={
            "predicate": _merge_predicate,
            "source_alias": "s",
            "target_alias": "t",
        },
    )
    table_merger.when_matched_update_all()
    table_merger.when_not_matched_insert_all()
    table_merger.when_not_matched_by_source_delete()
    table_merger.execute()
    partitioned = pl.read_delta(_writepath).sort("id")
    print(partitioned)
    return partitioned, pl


@app.cell
def _(new_data, old_data, pl):
    _writepath = "test_partitioned_write_and_merge.delta"
    pl.DataFrame(old_data).write_delta(
        _writepath, delta_write_options={"partition_by": "id"}
    )
    _merge_predicate = "\ns.id = t.id\n"
    print(
        pl.DataFrame(new_data)
        .write_delta(
            _writepath,
            mode="merge",
            delta_merge_options={
                "predicate": _merge_predicate,
                "source_alias": "s",
                "target_alias": "t",
            },
            delta_write_options={"partition_by": "id"},
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    partitioned_merge = pl.read_delta(_writepath).sort("id")
    print(partitioned_merge)
    return (partitioned_merge,)


@app.cell
def _(new_data, old_data, pl):
    _writepath = "test_merge.delta"
    pl.DataFrame(old_data).write_delta(
        _writepath, delta_write_options={"partition_by": "id"}
    )
    _merge_predicate = "\ns.id = t.id\n"
    print(
        pl.DataFrame(new_data)
        .write_delta(
            _writepath,
            mode="merge",
            delta_merge_options={
                "predicate": _merge_predicate,
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete()
        .execute()
    )
    simple = pl.read_delta(_writepath).sort("id")
    print(simple)
    return (simple,)


@app.cell
def _(partitioned, partitioned_merge, simple):
    from polars.testing import assert_frame_equal

    assert_frame_equal(partitioned_merge, partitioned)

    assert_frame_equal(simple, partitioned)
    return


if __name__ == "__main__":
    app.run()
