from retailco_lakehouse.transform.memberships import clean_memberships


def test_extracts_customer_id_from_filename(spark):
    bronze = spark.createDataFrame(
        [("dbfs:/Volumes/retailco/landing/operational_data/memberships/2024/1042.png", b"fake-image-bytes")],
        schema="path string, content binary",
    )

    row = clean_memberships(bronze).collect()[0]

    assert row["customer_id"] == "1042"
    assert row["membership_card"] == b"fake-image-bytes"
