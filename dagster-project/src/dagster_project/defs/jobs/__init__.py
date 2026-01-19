from dagster import (
    Definitions,
    AssetSelection,
    define_asset_job,
    job
)
from dagster_gcp.bigquery.ops import import_gcs_paths_to_bq

dbt_assets = AssetSelection.groups("dbt_models")
dbt_snapshots = AssetSelection.assets(
    ["snapshots", "snap_user_rfm"],
    ["snapshots", "snap_user_status"]
)

dbt_job = define_asset_job(
    name="dbt_job",
    selection=dbt_assets,
    description="Run dbt models"
)

dbt_snapshot_job = define_asset_job(
    name="dbt_snapshot_job",
    selection=dbt_snapshots,
    description="Run dbt snapshots to capture SCD Type 2 changes"
)

@job
def gcs_to_bq_load_job():
    import_gcs_paths_to_bq()

defs = Definitions(
    jobs=[
        dbt_job,
        dbt_snapshot_job,
        gcs_to_bq_load_job
    ]
)