from dagster import (
    run_status_sensor, 
    RunRequest, 
    DagsterRunStatus, 
    DefaultSensorStatus,
    RunStatusSensorContext
)
from dagster_project.defs.jobs import dbt_job, gcs_to_bq_load_job

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=dbt_job,
    monitored_jobs=[gcs_to_bq_load_job],
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60
)
def dbt_run_on_load_sensor(context: RunStatusSensorContext):

    return RunRequest(run_key=context.dagster_run.run_id)