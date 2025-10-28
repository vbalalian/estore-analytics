from dagster import (
    sensor, 
    DefaultSensorStatus, 
    SensorEvaluationContext,
    RunRequest,
    SkipReason
)
from dagster_gcp.gcs.sensor import get_gcs_keys
from dagster_project.defs.jobs import gcs_to_bq_load_job

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

    # Get the list of new files in the specified GCS bucket and prefix
    since_key = context.cursor or None
    new_gcs_keys = get_gcs_keys(
        bucket=GCS_BUCKET_NAME,
        since_key=since_key,
        gcs_session=context.resources.gcs
    )

    if not new_gcs_keys:
        return SkipReason(f"No new files found in GCS bucket: {GCS_BUCKET_NAME}.")

    last_key = new_gcs_keys[-1]

    for gcs_key in new_gcs_keys:
        yield RunRequest(
            run_key=gcs_key, 
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
    context.update_cursor(last_key)