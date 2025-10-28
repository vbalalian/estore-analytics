from dagster import Definitions
from dagster_project.defs.resources.gcp_resources import bq_resource, gcs_resource
from dagster_project.defs.resources.slack_resource import slack_resource

defs = Definitions(
    resources={
        "bigquery": bq_resource,
        "gcs": gcs_resource,
        "slack": slack_resource
    }
)