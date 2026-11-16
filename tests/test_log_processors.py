import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.log_processor import transform_logs

@pytest.fixture(scope="module")
def spark():
    """ Fixture to create a SparkSession for testing. """
    return SparkSession.builder.appName("TestLogProcessor").master("local[2]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1").getOrCreate()

def test_transform_logs_aggregation(spark):
    """ Test that log aggregation correctly filters and counts errors per endpoints. """

    data = [
        ("192.168.1.1", "2026-06-01 10:00:00", "/home", 200),    # Success (should be filtered out)
        ("192.168.1.2", "2026-06-01 10:01:00", "/login", 404),   # Error 1 for /login
        ("192.168.1.3", "2026-06-01 10:02:00", "/login", 404),   # Error 2 for /login
        ("192.168.1.4", "2026-06-01 10:03:00", "/api/data", 500) # Error 1 for /api/data
    ]

    columns = ["ip_address", "timestamp", "endpoint", "status_code"]
    df = spark.createDataFrame(data, columns)

    results_df = transform_logs(df)

    results = results_df.collect()

    login_row = next((r for r in results if r["endpoint"] == "/login" and r["status_code"] == 404), None)

    assert login_row is not None
    assert login_row["error_count"] == 2

    status_codes = {r["status_code"] for r in results}
    assert 200 not in status_codes

    print("Test passed: Log aggregation correctly filters and counts errors per endpoint.")