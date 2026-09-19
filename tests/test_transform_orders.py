from retailco_lakehouse.transform.orders import clean_orders, explode_order_items, parse_orders

# The source export writes order_date unquoted (e.g. `"order_date": 2024-05-01`),
# which is invalid JSON — clean_orders/parse_orders must fix that before parsing.
RAW_ORDER_LINE = (
    '{"customer_id": 500, '
    '"items": ['
    '{"category": "Electronics", "details": {"brand": "Acme", "color": "black"}, '
    '"item_id": 1, "name": "Widget", "price": 25, "quantity": 2}, '
    '{"category": "Electronics", "details": {"brand": "Acme", "color": "red"}, '
    '"item_id": 2, "name": "Gadget", "price": 40, "quantity": 1}'
    '], '
    '"order_date": 2024-05-01, '
    '"order_id": 900, '
    '"order_status": "Completed", '
    '"payment_method": "credit_card", '
    '"total_amount": 90, '
    '"transaction_timestamp": "2024-05-01T10:15:00"}'
)


def _bronze(spark, lines):
    return spark.createDataFrame([(line,) for line in lines], schema="value string")


def test_parse_orders_fixes_and_parses_the_malformed_date_field(spark):
    parsed = parse_orders(_bronze(spark, [RAW_ORDER_LINE]))
    row = parsed.collect()[0]

    assert row["json_value"] is not None
    assert row["json_value"]["order_id"] == 900
    assert row["json_value"]["order_date"] == "2024-05-01"
    assert len(row["json_value"]["items"]) == 2


def test_clean_orders_explodes_one_row_per_line_item(spark):
    result = clean_orders(_bronze(spark, [RAW_ORDER_LINE])).orderBy("item_id")

    assert result.count() == 2
    rows = result.collect()
    assert [r["item_id"] for r in rows] == [1, 2]
    assert rows[0]["name"] == "Widget"
    assert rows[0]["brand"] == "Acme"
    assert rows[0]["color"] == "black"
    assert rows[0]["order_id"] == 900
    assert rows[0]["customer_id"] == 500


def test_explode_order_items_deduplicates_identical_items(spark):
    duplicated_items_line = RAW_ORDER_LINE.replace(
        '"item_id": 2, "name": "Gadget", "price": 40, "quantity": 1}',
        '"item_id": 1, "name": "Widget", "price": 25, "quantity": 2}',
    )
    parsed = parse_orders(_bronze(spark, [duplicated_items_line]))

    result = explode_order_items(parsed)

    # array_distinct collapses the two identical item structs into one.
    assert result.count() == 1
