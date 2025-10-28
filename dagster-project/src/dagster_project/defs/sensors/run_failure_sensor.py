from dagster import run_failure_sensor, RunFailureSensorContext, DefaultSensorStatus
from dagster_slack import SlackResource
from dagster_project.defs.jobs import dbt_job, gcs_to_bq_load_job

@run_failure_sensor(
    monitored_jobs=[dbt_job, gcs_to_bq_load_job],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    request_job=None
)
def run_failure_sensor(context: RunFailureSensorContext, slack: SlackResource):
    failed_run = context.dagster_run
    job_name = failed_run.job_name
    run_id = failed_run.run_id
    error_message = failed_run.tags.get("dagster/step_failure_message", "No error message available.")

    context.log.error(
        f"Job '{job_name}' with Run ID '{run_id}' has failed. Error message: {error_message}"
    )

    slack_message = f"""
:x: *Dagster Run Failed*
*Job:* `{job_name}`
*Run ID:* `{run_id}`
""".strip()

    slack.get_client().chat_postMessage(
        channel="#estore-dagster-reports",
        text=slack_message
    )