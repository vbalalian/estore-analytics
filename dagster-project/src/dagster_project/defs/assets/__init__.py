from dagster import asset
from dagster_gcp import BigQueryResource
from google.cloud import bigquery

@asset
def raw_data_load(bq_resource: BigQueryResource):
    """Load CSV files from GCS to BigQuery"""

    # Construct a BigQuery client object.
    client = bq_resource.get_client()

    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "your-project.your_dataset.your_table_name"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("event_time", "TIMESTAMP"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("product_id", "INTEGER"),
            bigquery.SchemaField("category_id", "INTEGER"),
            bigquery.SchemaField("category_code", "STRING"),
            bigquery.SchemaField("brand", "STRING"),
            bigquery.SchemaField("price", "FLOAT"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("user_session", "STRING")
        ],
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND"
        ),
    
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states-by-date.csv"

    load_job = client.load_table_from_uri(
        source_uris=uri, 
        destination=table_id,
        job_config=job_config
        )

    load_job.result()

    table = client.get_table(table_id)
    
    print("Loaded {} rows to table {}".format(table.num_rows, table_id))