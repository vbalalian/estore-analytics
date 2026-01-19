"""Tests to verify all Dagster definitions load correctly."""


def test_jobs_load():
    """Verify all jobs can be imported without errors."""
    from dagster_project.defs.jobs import (
        dbt_job,
        dbt_snapshot_job,
        gcs_to_bq_load_job,
    )

    assert dbt_job is not None
    assert dbt_job.name == "dbt_job"

    assert dbt_snapshot_job is not None
    assert dbt_snapshot_job.name == "dbt_snapshot_job"

    assert gcs_to_bq_load_job is not None
    assert gcs_to_bq_load_job.name == "gcs_to_bq_load_job"


def test_sensors_load():
    """Verify all sensors can be imported without errors."""
    from dagster_project.defs.sensors.gcs_new_file_sensor import gcs_new_file_sensor
    from dagster_project.defs.sensors.dbt_run_on_load_sensor import dbt_run_on_load_sensor
    from dagster_project.defs.sensors.run_failure_sensor import run_failure_sensor

    assert gcs_new_file_sensor is not None
    assert gcs_new_file_sensor.name == "gcs_new_file_sensor"

    assert dbt_run_on_load_sensor is not None
    assert dbt_run_on_load_sensor.name == "dbt_run_on_load_sensor"

    assert run_failure_sensor is not None
    assert run_failure_sensor.name == "run_failure_sensor"


def test_schedules_load():
    """Verify all schedules can be imported without errors."""
    from dagster_project.defs.schedules import (
        daily_pipeline_schedule,
        weekly_snapshot_schedule,
    )

    assert daily_pipeline_schedule is not None
    assert weekly_snapshot_schedule is not None


def test_resources_load():
    """Verify all resources can be imported without errors."""
    from dagster_project.defs.resources.gcp_resources import gcs_resource, bq_resource
    from dagster_project.defs.resources.slack_resource import slack_resource

    assert gcs_resource is not None
    assert bq_resource is not None
    assert slack_resource is not None
