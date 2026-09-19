# S3 bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM role Databricks assumes to reach the bucket
# ---------------------------------------------------------------------------
# The external ID is generated once and stored in state — Databricks
# recommends a value unique to this credential (e.g. a UUID), rather than
# something predictable, since it's the only thing stopping another AWS
# account from assuming this role.

resource "random_uuid" "external_id" {}

data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  role_name      = var.iam_role_name
  external_id    = random_uuid.external_id.result
}

resource "aws_iam_role" "databricks_s3_access" {
  name               = var.iam_role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
  tags               = var.tags
}

data "databricks_aws_unity_catalog_policy" "this" {
  aws_account_id = data.aws_caller_identity.current.account_id
  bucket_name    = var.bucket_name
  role_name      = var.iam_role_name
}

resource "aws_iam_policy" "databricks_s3_access" {
  name   = "${var.iam_role_name}-policy"
  policy = data.databricks_aws_unity_catalog_policy.this.json
}

resource "aws_iam_role_policy_attachment" "databricks_s3_access" {
  role       = aws_iam_role.databricks_s3_access.name
  policy_arn = aws_iam_policy.databricks_s3_access.arn
}

# Unity Catalog storage credential + external location
# ---------------------------------------------------------------------------

resource "databricks_storage_credential" "this" {
  name = var.storage_credential_name

  aws_iam_role {
    role_arn = aws_iam_role.databricks_s3_access.arn
  }

  comment = "Managed by Terraform (infra/) for the RetailCo lakehouse."

  depends_on = [aws_iam_role_policy_attachment.databricks_s3_access]
}

resource "databricks_external_location" "this" {
  name            = var.external_location_name
  url             = "s3://${var.bucket_name}/"
  credential_name = databricks_storage_credential.this.id
  comment         = "Managed by Terraform (infra/) for the RetailCo lakehouse."
}
