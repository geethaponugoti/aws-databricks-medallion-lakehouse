from retailco_lakehouse.transform.addresses import clean_addresses


def test_pivots_shipping_and_billing_rows_to_one_row_per_customer(spark):
    bronze = spark.createDataFrame(
        [
            (1, "shipping", "1 Main St", "Springfield", "IL", "62701"),
            (1, "billing", "2 Oak Ave", "Springfield", "IL", "62702"),
        ],
        schema="customer_id long, address_type string, address_line_1 string, city string, "
        "state string, postcode string",
    )

    result = clean_addresses(bronze)
    row = result.collect()[0]

    assert result.count() == 1
    assert row["shipping_address_line_1"] == "1 Main St"
    assert row["billing_address_line_1"] == "2 Oak Ave"
    assert row["billing_postcode"] == "62702"


def test_missing_address_type_leaves_those_columns_null(spark):
    bronze = spark.createDataFrame(
        [(1, "shipping", "1 Main St", "Springfield", "IL", "62701")],
        schema="customer_id long, address_type string, address_line_1 string, city string, "
        "state string, postcode string",
    )

    row = clean_addresses(bronze).collect()[0]

    assert row["shipping_address_line_1"] == "1 Main St"
    assert row["billing_address_line_1"] is None
