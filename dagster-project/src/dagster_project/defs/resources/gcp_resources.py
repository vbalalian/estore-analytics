from dagster_gcp import GCSResource, bigquery_resource
from dagster import EnvVar

bq_resource = bigquery_resource.configured({
    "project":{"env":"GCP_PROJECT_ID"}
})

gcs_resource = GCSResource(
    project=EnvVar("GCP_PROJECT_ID")
)