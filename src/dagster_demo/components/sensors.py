import os
import dagster as dg

# FIXME: RUN REQUEST TARGETING ALL ASSETS SHOULD NOT BE LAUNCHING, TRY DROPPING FROM BRONZE ASSET


def detect_new_files_in_dir(context: dg.SensorEvaluationContext, directory: str):
    # find last modified date already processed
    cursor_last_file_modification_time = float(context.cursor) if context.cursor else 0
    request_runs = []

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            last_modified = os.path.getmtime(filepath)
            # only process fresh files
            if last_modified > cursor_last_file_modification_time:
                # Construct the RunRequest with run_key and config
                request_runs.append(f"{filename}:{last_modified}")
                cursor_last_file_modification_time = last_modified

    # Update the cursor
    context.update_cursor(str(cursor_last_file_modification_time))

    return request_runs
