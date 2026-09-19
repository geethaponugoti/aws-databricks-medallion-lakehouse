from retailco_lakehouse.transform.refunds import clean_refunds


def test_splits_refund_reason_into_reason_and_source(spark):
    bronze = spark.createDataFrame(
        [(1, 66, "2025-01-10T11:30:00", 85.75, "Payment Error:Retailer")],
        schema="refund_id int, payment_id int, refund_timestamp string, refund_amount double, "
        "refund_reason string",
    )

    row = clean_refunds(bronze).collect()[0]

    assert row["refund_reason"] == "Payment Error"
    assert row["refund_source"] == "Retailer"


def test_splits_refund_timestamp_into_date_and_time(spark):
    bronze = spark.createDataFrame(
        [(1, 66, "2025-01-10T11:30:00", 85.75, "Payment Error:Retailer")],
        schema="refund_id int, payment_id int, refund_timestamp string, refund_amount double, "
        "refund_reason string",
    )

    row = clean_refunds(bronze).collect()[0]

    assert str(row["refund_date"]) == "2025-01-10"
    assert row["refund_time"] == "11:30:00"
