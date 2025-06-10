import os
import dagster as dg


def detect_new_files_in_dir(context: dg.SensorEvaluationContext, directory: str):
    # find last modified date already processed
    cursor_last_file_modification_time = float(context.cursor) if context.cursor else 0
    max_mtime = cursor_last_file_modification_time

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            last_modified = os.path.getmtime(filepath)
            # only process fresh files
            if last_modified >= cursor_last_file_modification_time:
                # Construct the RunRequest with run_key and config
                run_key = f"{filename}:{last_modified}"
                # run_config = {"ops": {"my_asset": {"config": {"filename": filename}}}}
                # yield dg.RunRequest(run_key=run_key, run_config=run_config)
                yield dg.RunRequest(run_key=run_key)
                max_mtime = last_modified

    # Update the cursor
    context.update_cursor(str(max_mtime))
