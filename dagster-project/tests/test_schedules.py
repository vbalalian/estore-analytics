"""Tests to verify schedule configurations are correct."""


def test_daily_pipeline_schedule_targets_dbt_job():
    """Verify daily pipeline schedule targets the dbt job."""
    from dagster_project.defs.schedules import daily_pipeline_schedule
    from dagster_project.defs.jobs import dbt_job

    assert daily_pipeline_schedule.job == dbt_job
    assert daily_pipeline_schedule.cron_schedule is not None


def test_weekly_snapshot_schedule_targets_snapshot_job():
    """Verify weekly snapshot schedule targets the snapshot job."""
    from dagster_project.defs.schedules import weekly_snapshot_schedule
    from dagster_project.defs.jobs import dbt_snapshot_job

    assert weekly_snapshot_schedule.job == dbt_snapshot_job
    assert weekly_snapshot_schedule.cron_schedule is not None
