from dagster import Definitions, AssetSelection, define_asset_job

dbt_assets = AssetSelection.groups("dbt_models")

dbt_job = define_asset_job(
    name="dbt_job", 
    selection=dbt_assets,
    description="Run dbt models"
    )

gcs_to_bq_load_job = define_asset_job(
    name="gcs_to_bq_load_job",
    selection="gcs_to_bq_load_asset",
    description="Load CSV files from GCS to BigQuery"
    )

defs = Definitions(
    jobs=[
        dbt_job, 
        gcs_to_bq_load_job
        ]
)