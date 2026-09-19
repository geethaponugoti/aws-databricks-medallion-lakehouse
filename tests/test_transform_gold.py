from retailco_lakehouse.transform.gold import build_customer_address, build_monthly_order_summary


def test_build_customer_address_joins_on_customer_id(spark):
    customers = spark.createDataFrame(
        [(1, "Ada Lovelace", "1815-12-10", "ada@example.com", "555-0100")],
        schema="customer_id long, customer_name string, date_of_birth string, email string, telephone string",
    )
    addresses = spark.createDataFrame(
        [(1, "1 Main St", "Springfield", "IL", "62701", "2 Oak Ave", "Springfield", "IL", "62702")],
        schema="customer_id long, shipping_address_line_1 string, shipping_city string, "
        "shipping_state string, shipping_postcode string, billing_address_line_1 string, "
        "billing_city string, billing_state string, billing_postcode string",
    )

    row = build_customer_address(customers, addresses).collect()[0]

    assert row["customer_name"] == "Ada Lovelace"
    assert row["shipping_address_line_1"] == "1 Main St"
    assert row["billing_postcode"] == "62702"


def test_build_customer_address_drops_customers_without_an_address(spark):
    customers = spark.createDataFrame(
        [(1, "Ada Lovelace", "1815-12-10", "ada@example.com", "555-0100")],
        schema="customer_id long, customer_name string, date_of_birth string, email string, telephone string",
    )
    addresses = spark.createDataFrame(
        [], schema="customer_id long, shipping_address_line_1 string, shipping_city string, "
        "shipping_state string, shipping_postcode string, billing_address_line_1 string, "
        "billing_city string, billing_state string, billing_postcode string",
    )

    assert build_customer_address(customers, addresses).count() == 0


def test_build_monthly_order_summary_excludes_cancelled_and_pending(spark):
    orders = spark.createDataFrame(
        [
            (1, "Completed", "2024-05-01T10:00:00", 10, 2, 100),
            (2, "Cancelled", "2024-05-02T10:00:00", 10, 1, 50),
            (3, "Pending", "2024-05-03T10:00:00", 10, 1, 20),
        ],
        schema="order_id long, order_status string, transaction_timestamp timestamp, "
        "customer_id long, quantity long, price long",
    )

    result = build_monthly_order_summary(orders).collect()

    assert len(result) == 1
    row = result[0]
    assert row["total_orders"] == 1
    assert row["total_items"] == 2
    assert row["total_amount"] == 200


def test_build_monthly_order_summary_computes_total_amount_as_price_times_quantity(spark):
    orders = spark.createDataFrame(
        [
            (1, "Completed", "2024-05-01T10:00:00", 10, 3, 15),
            (1, "Completed", "2024-05-01T10:00:00", 10, 1, 100),
        ],
        schema="order_id long, order_status string, transaction_timestamp timestamp, "
        "customer_id long, quantity long, price long",
    )

    row = build_monthly_order_summary(orders).collect()[0]

    assert row["total_orders"] == 1  # same order_id, counted once via countDistinct
    assert row["total_items"] == 4
    assert row["total_amount"] == 145  # (3*15) + (1*100)
