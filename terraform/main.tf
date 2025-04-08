terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.15.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "terraform-fotmob-terra-bucket-kg" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_dataproc_cluster" "etl_cluster" {
  name   = var.dataproc_name#"epl-etl-cluster"
  region = var.region#"us-central1"
  
}

# resource "google_bigquery_dataset" "demo_dataset" {
#   dataset_id = var.bq_dataset_name
#   location   = var.location
# }