from dagster import asset, AssetKey
from dagster_gcp.bigquery.ops import import_gcs_paths_to_bq
# from dagster_gcp.bigquery.configs import define_bigquery_load_config

@asset(
        # config_schema = GcsToBqLoadConfig.to_config_schema()
        # required_resource_keys={"bigquery"},
        # key=AssetKey(["estore_raw", "events_sampled"]), ##### CHANGE AS NEEDED
        description="Load raw CSV data from GCS to BigQuery"

)
def gcs_to_bq_load_asset(context):
    """Load a single CSV file from GCS to BigQuery"""

    config = context.op_config

    import_gcs_paths_to_bq(
        # context,
        paths=[f"gs://{config['gcs_bucket']}/{config['gcs_key']}"]
        # bigquery_load_config=config.bigquery,
        # bigquery_resource=context.resources.bigquery
    )

    context.log.info("Data loaded to BigQuery")
