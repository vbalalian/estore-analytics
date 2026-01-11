output "vm_external_ip" {
  value       = google_compute_instance.vm_instance.network_interface[0].access_config[0].nat_ip
  description = "External IP address of the VM instance"
}

output "dagster_ui_url" {
  value       = "http://${google_compute_instance.vm_instance.network_interface[0].access_config[0].nat_ip}:3000"
  description = "URL for the Dagster web UI"
}

output "gcs_bucket_url" {
  value       = "gs://${google_storage_bucket.raw_data.name}"
  description = "GCS bucket URL for raw data uploads"
}

output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.raw.dataset_id
  description = "BigQuery dataset ID for raw data"
}

output "service_account_email" {
  value       = google_service_account.tf-sa.email
  description = "Service account email - create a key for this account in GCP Console"
}
