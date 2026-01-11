variable "project" {
  type        = string
  description = "The Google Cloud project ID to deploy resources into."
}

variable "region" {
  type        = string
  description = "The Google Cloud region to deploy resources into."
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "The Google Cloud zone to deploy resources into."
  default     = "us-central1-a"
}

variable "gcs_bucket_name" {
  type        = string
  description = "Globally unique name for the GCS bucket used to store raw data."
}