variable "credentials" {
  description = "Project Credentials"
  default     = "./terraform_keys/terraform_keys.json"
}

variable "project" {
  description = "Project: Fotmob ETL"
  default     = "rare-habitat-447201-d6"
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
  default = "pl-etl-cluster"
  
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