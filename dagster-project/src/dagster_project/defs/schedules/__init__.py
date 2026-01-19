from dagster import ScheduleDefinition, DefaultScheduleStatus
from dagster_project.defs.jobs import dbt_job, dbt_snapshot_job


daily_pipeline_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="0 0 * * *",  # every day, at midnight
    default_status=DefaultScheduleStatus.RUNNING,
)

weekly_snapshot_schedule = ScheduleDefinition(
    job=dbt_snapshot_job,
    cron_schedule="0 1 * * 0",  # every Sunday at 1 AM
    default_status=DefaultScheduleStatus.RUNNING,
)