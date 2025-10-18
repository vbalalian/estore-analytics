from dagster import Definitions
from dagster_project.defs.resources.gcp_resources import bq_resource, gcs_resource

defs = Definitions(
    resources={
        "bq_resource": bq_resource,
        "gcs_resource": gcs_resource
    }
)