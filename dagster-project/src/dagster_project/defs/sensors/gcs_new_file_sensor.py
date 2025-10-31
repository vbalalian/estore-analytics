from dagster import (
    sensor, 
    DefaultSensorStatus, 
    SensorEvaluationContext,
    RunRequest,
    SkipReason,
    RunsFilter
)
from dagster_gcp.gcs.sensor import get_gcs_keys
from dagster_project.defs.jobs import gcs_to_bq_load_job
import time

GCS_BUCKET_NAME = "ecommerce65465"
DESTINATION_TABLE = "mkt-analytics-project.estore_raw.events"

@sensor(
        job=gcs_to_bq_load_job,
        minimum_interval_seconds=300,
        default_status=DefaultSensorStatus.RUNNING,
        required_resource_keys={"gcs"}
        )
def gcs_new_file_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors a GCS bucket for new files and triggers a job to load raw data
    when new files are detected.
    """
    try:
        # Verify last run succeeded, and if it did, advance the cursor
        instance = context.instance

        runs = instance.get_runs(
            filters=RunsFilter(
                job_name=gcs_to_bq_load_job.name,
                tags={"dagster/sensor_name": context.sensor_name}
            ),
            limit=1
        )

        last_run = runs[0] if runs else None

        if last_run:
            if last_run.is_success:
                new_cursor = last_run.tags.get("latest_gcs_key")
                if new_cursor and new_cursor != context.cursor:
                    context.log.info(
                        f"Last run succeeded. Advancing cursor from "
                        f"{context.cursor} -> {new_cursor}"
                    )
                    context.update_cursor(new_cursor)
            else:
                context.log.info(
                    f"Last run failed (status: {last_run.status}). "
                    f"Keeping cursor at {context.cursor} to retry."
                )

        # Get the list of new files in the specified GCS bucket and prefix

        since_key = context.cursor or None

        new_gcs_keys = get_gcs_keys(
            bucket=GCS_BUCKET_NAME,
            since_key=since_key,
            gcs_session=context.resources.gcs
        )

        if not new_gcs_keys:
            return SkipReason(f"No new files found in GCS bucket: {GCS_BUCKET_NAME}.")

        context.log.info(f"Found {len(new_gcs_keys)} new file(s): {new_gcs_keys}")

    except Exception as e:
        context.log.error(f"Sensor execution failed: {str(e)}")
        
        return SkipReason(f"Sensor error: {str(e)}")

    last_key = new_gcs_keys[-1]

    for gcs_key in new_gcs_keys:
        yield RunRequest(
            run_key=f"{gcs_key}_{int(time.time())}", 
            run_config={
                "ops": {
                    "import_gcs_paths_to_bq": {
                        "inputs": {
                            "paths": [f"gs://{GCS_BUCKET_NAME}/{gcs_key}"]
                        },
                        "config": {
                                "destination": DESTINATION_TABLE,
                                "load_job_config": {
                                    "source_format": "CSV",
                                    "autodetect": True,
                                    "skip_leading_rows": 1,
                                    "write_disposition": "WRITE_APPEND"
                            }
                        }
                    }
                },
        },
            tags={
                "gcs_key": gcs_key,
                "latest_gcs_key": last_key,
                "file_count": str(len(new_gcs_keys))
            }
        )