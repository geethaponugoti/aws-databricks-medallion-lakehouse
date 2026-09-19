# S3 + Unity Catalog setup (manual walkthrough)

This document is the click-through version of what [infra/](../infra/) automates with Terraform. Use it if you want to understand what Terraform is doing under the hood, need to set things up without Terraform, or are troubleshooting a broken connection.

If you just want things provisioned, run `terraform apply` in [infra/](../infra/) instead and skip to [Verification checklist](#verification-checklist).

## Prerequisites

- An AWS account
- An S3 bucket created for this project (referred to below as `YOUR-BUCKET-NAME` — never hardcode this in notebooks or source; it's passed in as the `bucket_name` bundle/job parameter)
- A Databricks workspace on AWS with Unity Catalog enabled

## Option: Unity Catalog external location (recommended)

This centralizes credentials and enables governance, instead of embedding AWS keys in cluster configs.

### 1. In the AWS Console

- Go to **IAM → Roles → Create Role**
- Select **AWS account → Another AWS account**
- Enter the Databricks AWS account ID: `414351767826`
- Do **not** check "Require external ID" yet — you'll add that after Databricks generates it
- Click **Next**

### 2. Attach a permissions policy

Create a policy (JSON), replacing `YOUR-BUCKET-NAME`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME/*",
        "arn:aws:s3:::YOUR-BUCKET-NAME"
      ]
    }
  ]
}
```

Attach it to the role, and copy the role's ARN (`arn:aws:iam::<account-id>:role/<role-name>`).

### 3. In Databricks

- **Catalog Explorer → External Data → Storage Credentials → Create Credential**
- Paste the Role ARN
- Copy the **External ID** Databricks generates

### 4. Back in AWS IAM

Edit the role's trust policy to require that external ID:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "YOUR-EXTERNAL-ID-FROM-DATABRICKS"
        }
      }
    }
  ]
}
```

### 5. Create the external location in Databricks

- **Catalog Explorer → External Data → External Locations → Create Location**
- URL: `s3://YOUR-BUCKET-NAME/`
- Storage Credential: the one created above

## Verification checklist

**IAM role permissions policy** (IAM → Roles → your role → Permissions):
- [ ] `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `s3:GetBucketLocation`
- [ ] Resource includes both `arn:aws:s3:::YOUR-BUCKET-NAME/*` and `arn:aws:s3:::YOUR-BUCKET-NAME`

**IAM role trust relationship** (IAM → Roles → your role → Trust relationships):
- [ ] Principal is Databricks' Unity Catalog master role
- [ ] Condition includes `sts:ExternalId` matching the value Databricks shows for your Storage Credential

**Databricks Storage Credential** (Catalog Explorer → External Data → Storage Credentials):
- [ ] IAM Role ARN matches the role you created
- [ ] Status shows **Validated**

**Databricks External Location** (Catalog Explorer → External Data → External Locations):
- [ ] URL is `s3://YOUR-BUCKET-NAME/` (trailing slash)
- [ ] Status shows **Active**

### Quick fixes

| Symptom | Fix |
|---|---|
| Storage Credential won't validate | External ID mismatch — update the AWS trust policy with the exact value Databricks shows |
| External Location stuck on "Pending" | Its Storage Credential isn't validated yet — validate that first |
| "Access Denied" on test connection | An S3 action is missing from the IAM policy |
| "Invalid bucket" / `NoSuchBucket` | Bucket name typo, or the bucket doesn't exist in this account/region |

## End-to-end connectivity check

Once the credential is validated and the external location is active, run `notebooks/setup/01_verify_s3_connection.py` — it lists the bucket and does a round-trip write/read/delete of a test object, and prints a diagnosis if any step fails.
