from dagster_gcp import BigQueryResource, GCSResource
from dagster import EnvVar

gcp_creds = EnvVar("GCP_CREDS")
project_id = EnvVar("GCP_PROJECT_ID")

bq_resource = BigQueryResource(
    project=project_id,
    gcp_credentials=gcp_creds
)

gcs_resource = GCSResource(
    project=project_id
)