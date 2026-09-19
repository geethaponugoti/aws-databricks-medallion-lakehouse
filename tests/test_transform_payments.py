from retailco_lakehouse.transform.payments import clean_payments


def test_decodes_payment_status_codes(spark):
    bronze = spark.createDataFrame(
        [
            (1, 100, "2024-03-15T14:30:00", "1", "credit_card"),
            (2, 101, "2024-03-16T09:00:00", "3", "paypal"),
        ],
        schema="payment_id int, customer_id int, payment_date string, payment_status string, "
        "payment_method string",
    )

    result = clean_payments(bronze).orderBy("payment_id").collect()

    assert result[0]["payment_status"] == "Success"
    assert result[1]["payment_status"] == "Cancelled"


def test_splits_payment_date_into_date_and_time(spark):
    bronze = spark.createDataFrame(
        [(1, 100, "2024-03-15T14:30:00", "1", "credit_card")],
        schema="payment_id int, customer_id int, payment_date string, payment_status string, "
        "payment_method string",
    )

    row = clean_payments(bronze).collect()[0]

    assert str(row["payment_date"]) == "2024-03-15"
    assert row["payment_time"] == "14:30:00"


def test_unknown_status_code_maps_to_null(spark):
    bronze = spark.createDataFrame(
        [(1, 100, "2024-03-15T14:30:00", "9", "credit_card")],
        schema="payment_id int, customer_id int, payment_date string, payment_status string, "
        "payment_method string",
    )

    row = clean_payments(bronze).collect()[0]

    assert row["payment_status"] is None
