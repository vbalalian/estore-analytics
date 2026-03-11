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


def test_omni_component_yaml():
    """Verify the Omni component defs.yaml is valid and well-structured."""
    from pathlib import Path
    import yaml

    defs_yaml = (
        Path(__file__).parent.parent
        / "src"
        / "dagster_project"
        / "defs"
        / "omni_ingest"
        / "defs.yaml"
    )
    assert defs_yaml.exists(), "omni_ingest/defs.yaml not found"

    with open(defs_yaml) as f:
        config = yaml.safe_load(f)

    assert config["type"] == "dagster_project.components.custom_omni.CustomOmniComponent"
    assert "workspace" in config["attributes"]
    assert "base_url" in config["attributes"]["workspace"]
    assert "api_key" in config["attributes"]["workspace"]


def test_topic_yaml_parsing():
    """Verify topic YAML parsing extracts correct join dependencies."""
    from dagster_project.components.custom_omni import _build_topic_deps
    from pathlib import Path

    omni_dir = Path(__file__).parent.parent.parent / "omni" / "BigQuery"
    if not omni_dir.exists():
        import pytest
        pytest.skip("Omni topic files not available")

    topic_deps = _build_topic_deps(omni_dir)

    # Sessions topic: base + dim_users + dim_user_rfm
    assert "fct_sessions" in topic_deps
    assert set(topic_deps["fct_sessions"]) == {"fct_sessions", "dim_users", "dim_user_rfm"}

    # Customers topic: base + dim_user_rfm
    assert "dim_users" in topic_deps
    assert set(topic_deps["dim_users"]) == {"dim_users", "dim_user_rfm"}

    # Events topic: base + dim_users + dim_user_rfm + dim_products
    assert "fct_events" in topic_deps
    assert set(topic_deps["fct_events"]) == {"fct_events", "dim_users", "dim_user_rfm", "dim_products"}


def test_extract_join_views_nested():
    """Verify recursive join extraction works for nested joins."""
    from dagster_project.components.custom_omni import _extract_join_views

    joins = {
        "omni_dbt_marts__dim_users": {
            "omni_dbt_marts__dim_user_rfm": {}
        },
        "omni_dbt_marts__dim_products": {}
    }
    views = _extract_join_views(joins)
    assert set(views) == {
        "omni_dbt_marts__dim_users",
        "omni_dbt_marts__dim_user_rfm",
        "omni_dbt_marts__dim_products"
    }


def test_custom_omni_dbt_key_lookup():
    """Verify dbt manifest lookup produces correct asset keys with schema prefix."""
    from dagster import AssetKey
    from dagster_project.components.custom_omni import _build_dbt_key_lookup
    from pathlib import Path

    manifest_path = Path(__file__).parent.parent.parent / "dbt-project" / "target" / "manifest.json"
    if not manifest_path.exists():
        import pytest
        pytest.skip("dbt manifest not available")

    lookup = _build_dbt_key_lookup(manifest_path)

    assert lookup["fct_sessions"] == AssetKey(["marts", "fct_sessions"])
    assert lookup["dim_users"] == AssetKey(["marts", "dim_users"])
    assert lookup["fct_events"] == AssetKey(["marts", "fct_events"])
    assert lookup["dim_products"] == AssetKey(["marts", "dim_products"])
    assert lookup["dim_user_rfm"] == AssetKey(["marts", "dim_user_rfm"])
    assert lookup["snap_user_rfm"] == AssetKey(["snapshots", "snap_user_rfm"])
