# Infrastructure Setup

Terraform configuration for provisioning the GCP infrastructure required to run this project.

## Resources Created

| Resource | Description |
|----------|-------------|
| `google_compute_instance` | VM instance running Dagster |
| `google_compute_network` | VPC network |
| `google_compute_firewall` | Firewall rules for SSH and Dagster UI |
| `google_storage_bucket` | GCS bucket for raw data |
| `google_bigquery_dataset` | BigQuery dataset for raw data |
| `google_service_account` | Service account with required IAM roles |

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- A GCP project with billing enabled

## Setup

1. **Authenticate with GCP**
   ```bash
   gcloud auth application-default login
   ```

2. **Configure variables**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```
   Edit `terraform.tfvars` with your values:
   - `project` - Your GCP project ID
   - `gcs_bucket_name` - A globally unique bucket name

3. **Initialize and apply**
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

4. **Create service account key**

   After apply, create a key for the service account:
   - Go to GCP Console → IAM & Admin → Service Accounts
   - Find the service account (email shown in Terraform output)
   - Actions → Manage keys → Add key → Create new key → JSON
   - Save the key file securely (do not commit to version control)

5. **Note the outputs**

   Terraform will display:
   - `vm_external_ip` - SSH access and Dagster URL
   - `dagster_ui_url` - Direct link to Dagster web UI
   - `gcs_bucket_url` - Where to upload raw data
   - `service_account_email` - For creating the key above

## Cleanup

To destroy all resources:
```bash
terraform destroy
```
