import os
import dagster as dg

@dg.sensor(
    minimum_interval_seconds=5, #customize polling interval
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def new_file_sensor(directory: str):
    new_files = check_for_new_files()
    # New files, run `my_job`
    if new_files:
        for filename in new_files:
            yield dg.RunRequest(run_key=filename)
    # No new files, skip the run and log the reason
    else:
        yield dg.SkipReason("No new files found")

@dg.definitions
def sensors() -> dg.Definitions:
    return dg.Definitions(sensors={})