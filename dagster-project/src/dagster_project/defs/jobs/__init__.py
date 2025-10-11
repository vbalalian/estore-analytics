from dagster import AssetSelection, define_asset_job

dbt_assets = AssetSelection.groups("dbt_models")

dbt_job = define_asset_job(
    name="dbt_job",
    selection=dbt_assets
)