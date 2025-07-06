variable "credentials" {
  description = "Project Credentials"
  default     = "./terraform_keys/alert-rush-458419-c2-be92140d2382.json"
}

variable "project" {
  description = "Project: Fotmob ETL"
  default     = "alert-rush-458419-c2"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "dataproc_name" {
  description = "DataProc Name"
  default     = "etl-cluster"

}
# variable "bq_dataset_name" {
#   description = "My Big Query Dataset"
#   default     = "demo_dataset"
# }

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-fotmob-terra-bucket-kg"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "dataproc_service_account" {
  description = "DataProc IAM User Name"
  default = "epl-fotmob@alert-rush-458419-c2.iam.gserviceaccount.com"
}