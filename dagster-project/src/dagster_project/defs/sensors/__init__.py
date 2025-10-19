from dagster import sensor, DefaultSensorStatus, SensorEvaluationContext
from dagster_project.defs.resources.gcp_resources import GCSResource
from dagster_gcp.gcs.sensor import get_gcs_keys
from dagster import RunRequest, SkipReason


@sensor(
        name="gcs_new_files_sensor",
        job="raw_data_load_job",
        minimum_interval_seconds=300,
        default_status=DefaultSensorStatus.RUNNING
        )
def gcs_csv_sensor(context:SensorEvaluationContext, gcs:GCSResource):
    """
    Monitors GCS bucket for new CSV files.
    When new files are detected, triggers the raw_data_load_job.
    """
    gcs_client = gcs.get_client()
    since_key = context.cursor or None
    
    new_gcs_keys = get_gcs_keys(
        bucket=GCS_BUCKET,
        prefix=GCS_PREFIX,
        since_key=since_key,
        gcs_session=gcs_client
    )
    
    csv_keys = [key for key in new_gcs_keys if key.endswith('.csv')]
    
    if not csv_keys:
        return SkipReason(f"No new CSV files found in gs://{GCS_BUCKET}/{GCS_PREFIX}")
    
    context.log.info(f"Found {len(csv_keys)} new CSV file(s)")
    
    last_key = csv_keys[-1]
    context.update_cursor(last_key)
    
    # This RunRequest triggers the raw_data_load_job
    return RunRequest(
        run_key=f"gcs_load_{last_key}",
        tags={
            "source": "gcs_sensor",
            "new_files_count": str(len(csv_keys)),
            "latest_file": last_key,
        }
    )

