variable "bucket_name" {
  description = "Name of the S3 bucket backing the lakehouse. Must be globally unique. Never hardcode this elsewhere — it flows into databricks.yml and notebook parameters instead."
  type        = string
}

variable "aws_region" {
  description = "AWS region to create the S3 bucket and IAM role in."
  type        = string
  default     = "us-east-1"
}

variable "iam_role_name" {
  description = "Name of the IAM role Databricks assumes to read/write the bucket."
  type        = string
  default     = "retailco-databricks-s3-access-role"
}

variable "storage_credential_name" {
  description = "Name of the Unity Catalog storage credential."
  type        = string
  default     = "retailco-s3-credential"
}

variable "external_location_name" {
  description = "Name of the Unity Catalog external location."
  type        = string
  default     = "retailco-s3-location"
}

variable "tags" {
  description = "Tags applied to every resource this module creates."
  type        = map(string)
  default = {
    project = "aws-databricks-medallion-lakehouse"
  }
}
