from dagster import AssetSpec, AssetKey

GCS_BUCKET_NAME = "ecommerce65465"
DESTINATION_TABLE = "mkt-analytics-project.estore_raw.events"

group_name = "upstream_assets"

gcs_raw_files = AssetSpec(
    key=AssetKey(["gcs", "raw_events_files"]),
    group_name=group_name,
    description=f"Raw event CSV files in GCS bucket: {GCS_BUCKET_NAME}",
    metadata={
        "bucket": GCS_BUCKET_NAME,
        "format": "CSV",
        "refresh_frequency": "continuous",
    }
)

raw_events_table = AssetSpec(
    key=AssetKey(["estore_raw", "events"]),
    deps=[gcs_raw_files],
    group_name=group_name,
    description="Raw events table in BigQuery loaded from GCS",
    metadata={
        "table": DESTINATION_TABLE,
        "loaded_by": "gcs_to_bq_load_job",
    }
)
