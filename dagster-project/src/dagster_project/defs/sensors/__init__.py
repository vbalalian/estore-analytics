from dagster import Definitions
from dagster_project.defs.sensors.gcs_new_file_sensor import gcs_new_file_sensor
from dagster_project.defs.sensors.dbt_run_on_load_sensor import dbt_run_on_load_sensor

defs = Definitions(
    sensors=[
        gcs_new_file_sensor,
        dbt_run_on_load_sensor
    ]
)