from dagster import Definitions
from dagster_project.defs.assets.source_assets import gcs_raw_files, raw_events_table

defs = Definitions(
    assets=[
        gcs_raw_files,
        raw_events_table
    ]
)