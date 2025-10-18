from dagster import asset
from dagster_gcp import BigQueryResource

@asset
def test_asset(bq_resource: BigQueryResource):
    with bq_resource.get_client() as client:
        result = client.query(
            "SELECT * FROM `mkt-analytics-project.estore_marts.dim_categories` limit 10"
            )
        return result