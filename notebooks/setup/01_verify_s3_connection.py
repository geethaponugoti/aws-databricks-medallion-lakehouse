# Databricks notebook source
# MAGIC %md
# MAGIC # Verify S3 connection
# MAGIC Diagnostic check that the Unity Catalog external location backing this project is
# MAGIC reachable: lists the bucket, then round-trips a small test file. Run this after
# MAGIC completing [docs/s3_setup.md](../../docs/s3_setup.md) (or `terraform apply` in
# MAGIC `infra/`) and before running the pipeline for the first time.

# COMMAND ----------

dbutils.widgets.text("bucket_name", "", "S3 bucket name")
bucket_name = dbutils.widgets.get("bucket_name")
assert bucket_name, "Set the bucket_name widget/parameter before running this check."

# COMMAND ----------

print("=" * 70)
print("S3 CONNECTION DIAGNOSTIC")
print("=" * 70)

print(f"\nTest 1: listing s3://{bucket_name}/")
try:
    files = dbutils.fs.ls(f"s3://{bucket_name}/")
    print(f"OK — connected, found {len(files)} top-level item(s).")
    for f in files[:5]:
        print(f"  - {f.name}")
except Exception as e:
    error_msg = str(e)
    print(f"FAILED: {error_msg[:200]}...")
    if "Access Denied" in error_msg or "403" in error_msg:
        print("Diagnosis: IAM permission issue — check the role's trust policy (External ID) "
              "and that the S3 permissions policy is attached.")
    elif "InvalidAccessKeyId" in error_msg:
        print("Diagnosis: the Role ARN registered in Databricks may be wrong.")
    elif "No such bucket" in error_msg or "NoSuchBucket" in error_msg:
        print(f"Diagnosis: '{bucket_name}' doesn't exist or the name is wrong.")
    else:
        print("Diagnosis: see the error message above.")
    raise

# COMMAND ----------

print(f"\nTest 2: round-trip write/read/delete against s3://{bucket_name}/_connectivity_check/")
test_path = f"s3://{bucket_name}/_connectivity_check/test.txt"
try:
    dbutils.fs.put(test_path, "connectivity check", overwrite=True)
    content = dbutils.fs.head(test_path)
    assert content == "connectivity check"
    dbutils.fs.rm(test_path)
    print("OK — write, read, and delete all succeeded.")
    print("\nAll checks passed. The bucket is ready to use.")
except Exception as e:
    print(f"FAILED: {str(e)[:200]}...")
    print("Diagnosis: read access works but write/delete doesn't — check s3:PutObject / "
          "s3:DeleteObject are in the IAM policy.")
    raise
