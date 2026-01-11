terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.14.1"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "google_service_account" "tf-sa" {
  account_id   = "terraform-sa"
  display_name = "Terraform Service Account"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}

resource "google_compute_instance" "vm_instance" {
  name         = "terraform-instance"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-12-bookworm-v20250910"
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.name
    access_config {
    }
  }

  service_account {
    email  = google_service_account.tf-sa.email
    scopes = ["cloud-platform"]
  }
}

resource "google_storage_bucket" "raw_data" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
}