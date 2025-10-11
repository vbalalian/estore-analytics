from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_project.defs.jobs import dbt_job


daily_pipeline_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="0 0 * * *",  # every day, at midnight
    default_status=DefaultScheduleStatus.RUNNING
)