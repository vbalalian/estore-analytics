from dagster import (
    Definitions, 
    AssetSelection, 
    define_asset_job, 
    job
)
from dagster_gcp.bigquery.ops import import_gcs_paths_to_bq

dbt_assets = AssetSelection.groups("dbt_models")

dbt_job = define_asset_job(
    name="dbt_job", 
    selection=dbt_assets,
    description="Run dbt models"
    )

@job
def gcs_to_bq_load_job():
    import_gcs_paths_to_bq()

defs = Definitions(
    jobs=[
        dbt_job, 
        gcs_to_bq_load_job
        ]
)