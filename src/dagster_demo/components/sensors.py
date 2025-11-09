import os
import dagster as dg


def detect_new_files_in_dir(
    context: dg.SensorEvaluationContext, directory: str
) -> list[str]:
    """
    Detect new files in a directory based on modification time.
    Returns a list of absolute file paths for files that are newer than the cursor.
    """
    # find last modified date already processed
    cursor_last_file_modification_time = float(context.cursor) if context.cursor else 0
    new_file_paths = []
    max_modification_time = cursor_last_file_modification_time

    # First pass: collect all new files and find the maximum modification time
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            last_modified = os.path.getmtime(filepath)
            # only process fresh files
            if last_modified > cursor_last_file_modification_time:
                new_file_paths.append(filepath)
                max_modification_time = max(max_modification_time, last_modified)

    # Update the cursor to the newest file only after processing all files
    if new_file_paths:
        context.update_cursor(str(max_modification_time))

    return new_file_paths


def process_new_partitions_in_files(
    partition_keys: list[str], asset_name: str
) -> list[dg.RunRequest]:
    """After calling `detect_new_files_in_dir`, the calling sensor can call this function to get a list of partition keys to target.
    The sensor itself is responsible for parsing the files and supplying a list of partition keys."""
    run_requests = []

    for partition in partition_keys:
        run_requests.append(
            dg.RunRequest(
                run_key=f"{asset_name}_{str(partition)}",
                partition_key=str(partition),
            )
        )

    return run_requests
