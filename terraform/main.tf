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

resource "google_dataproc_cluster" "etl-cluster" {

  name    = var.dataproc_name
  project = var.project
  region  = var.region

  cluster_config {
    staging_bucket = var.gcs_bucket_name
    temp_bucket    = var.gcs_bucket_name
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
    }

  }
}


resource "google_storage_bucket_iam_member" "dataproc_bucket_access" {
  bucket = google_storage_bucket.terraform-fotmob-terra-bucket-kg.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dataproc_service_account}"
}


# resource "google_service_account_iam_member" "dataproc_sa_user_binding" {
#   # The ID of the service account you want the cluster to run as
#   service_account_id = "projects/rare-habitat-447201-d6/serviceAccounts/fotmob-epl-etl@rare-habitat-447201-d6.iam.gserviceaccount.com"

#   role   = "roles/iam.serviceAccountUser"

#   # The identity running 'terraform apply'
#   # Replace with the correct user or service account email
#   member = "user:fotmob-epl-etl@rare-habitat-447201-d6.iam.gserviceaccount.com"
# }