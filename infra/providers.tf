# Credentials for both providers come from the standard environment
# variables / CLI profiles — never from files in this repo:
#   AWS:        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or an AWS_PROFILE)
#   Databricks: DATABRICKS_HOST / DATABRICKS_TOKEN (or `databricks auth login`)

provider "aws" {
  region = var.aws_region
}

# Workspace-scoped auth is enough to manage Unity Catalog storage credentials
# and external locations via this provider — DATABRICKS_HOST/DATABRICKS_TOKEN
# should point at a workspace on the Unity Catalog metastore you want these
# registered against.
provider "databricks" {}

data "aws_caller_identity" "current" {}
