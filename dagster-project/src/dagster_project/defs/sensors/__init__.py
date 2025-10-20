from dagster import Definitions
from dagster_project.defs.sensors.gcs_new_file_sensor import gcs_new_file_sensor

defs = Definitions(
    sensors=[
        gcs_new_file_sensor
    ]
)