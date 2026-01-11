from dagster import (
    run_status_sensor,
    RunRequest,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    SkipReason
)
from dagster_project.defs.jobs import dbt_job, gcs_to_bq_load_job
from datetime import datetime

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    request_job=dbt_job,
    monitored_jobs=[gcs_to_bq_load_job],
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=300
)
def dbt_run_on_load_sensor(context: RunStatusSensorContext):
    try:
        # Set run_key to current minute to avoid multiple runs in the same interval
        time_window = (
            datetime.now().strftime('%Y%m%d_%H')
            + str(datetime.now().minute // 15)
        )

        return RunRequest(
            run_key=f"dbt_{time_window}",
            tags={"triggered_by": context.dagster_run.tags.get("gcs_key")}
        )
    except Exception as e:
        context.log.error(f"Sensor execution failed: {str(e)}")
        return SkipReason(f"Sensor error: {str(e)}")