from dagster import AssetSelection, define_asset_job

dbt_assets = AssetSelection.groups("dbt_models")

dbt_job = define_asset_job(
    name="dbt_job", 
    selection=dbt_assets,
    description="Run dbt models"
    )

raw_data_load_job = define_asset_job(
    name="raw_data_load_job",
    selection="raw_data_load",
    description="Load CSV files from GCS to BigQuery"
    )
