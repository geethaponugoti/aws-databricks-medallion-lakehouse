from retailco_lakehouse.transform.customers import clean_customers

SCHEMA = (
    "customer_id long, customer_name string, date_of_birth string, email string, "
    "member_since string, telephone string, created_timestamp string"
)


def _customer(customer_id, email="ada@example.com", created_timestamp="2024-01-01T00:00:00"):
    return (customer_id, "Ada Lovelace", "1815-12-10", email, "2020-01-01", "555-0100", created_timestamp)


def test_drops_rows_with_null_customer_id(spark):
    bronze = spark.createDataFrame(
        [_customer(1), _customer(None, email="ghost@example.com")], schema=SCHEMA
    )

    result = clean_customers(bronze)

    assert result.count() == 1
    assert result.collect()[0]["customer_id"] == 1


def test_keeps_only_the_latest_record_per_customer(spark):
    bronze = spark.createDataFrame(
        [
            _customer(1, email="old@example.com", created_timestamp="2023-01-01T00:00:00"),
            _customer(1, email="new@example.com", created_timestamp="2024-06-01T00:00:00"),
        ],
        schema=SCHEMA,
    )

    result = clean_customers(bronze)

    assert result.count() == 1
    assert result.collect()[0]["email"] == "new@example.com"


def test_casts_date_and_timestamp_columns(spark):
    bronze = spark.createDataFrame([_customer(1)], schema=SCHEMA)

    result = clean_customers(bronze)
    row = result.collect()[0]

    assert result.schema["date_of_birth"].dataType.typeName() == "date"
    assert result.schema["member_since"].dataType.typeName() == "date"
    assert result.schema["created_timestamp"].dataType.typeName() == "timestamp"
    assert str(row["date_of_birth"]) == "1815-12-10"
