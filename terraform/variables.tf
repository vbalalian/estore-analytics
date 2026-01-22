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

variable "deploy_user" {
  type        = string
  description = "Unix user for deployment. Created by startup script if it doesn't exist."
  default     = "dagster"
}

variable "repo_url" {
  type        = string
  description = "Git repository URL to clone for the estore-analytics project."
  default     = "https://github.com/vbalalian/estore-analytics"
}

variable "slack_bot_token" {
  type        = string
  description = "Slack bot token for pipeline notifications."
  sensitive   = true
}