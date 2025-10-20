from dagster import Definitions
from dagster_project.defs.assets.gcs_to_bq_load_asset import gcs_to_bq_load_asset

defs = Definitions(
    assets=[
        gcs_to_bq_load_asset
    ]
)