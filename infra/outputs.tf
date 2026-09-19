output "bucket_name" {
  description = "S3 bucket name — pass this as the bucket_name variable to the Databricks Asset Bundle."
  value       = aws_s3_bucket.lakehouse.bucket
}

output "iam_role_arn" {
  description = "ARN of the IAM role Databricks assumes to access the bucket."
  value       = aws_iam_role.databricks_s3_access.arn
}

output "storage_credential_name" {
  description = "Name of the Unity Catalog storage credential."
  value       = databricks_storage_credential.this.name
}

output "external_location_name" {
  description = "Name of the Unity Catalog external location."
  value       = databricks_external_location.this.name
}

output "external_location_url" {
  description = "S3 URL registered as the Unity Catalog external location."
  value       = databricks_external_location.this.url
}
