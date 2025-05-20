import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col

from src.etl.transformer import transform_data # Adjust path to your function

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestTransform").getOrCreate()

def test_transform_data(spark):
    # Input Data
    input_data = [
        (1, "Alice", 34, "usa", "2024-01-01"),
        (2, "Bob", 28, "canada", "2024-01-03"),
        (3, "Charlie", 45, "uk", "2024-01-05"),
        (4, "Diana", 25, "germany", "2024-01-07")
    ]

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("created_at", StringType(), True)
    ])

    input_df = spark.createDataFrame(data=input_data, schema=schema)

    # Run transformation
    result_df = transform_data(input_df)

    # Expected values to check
    result = result_df.select("id", "age_group", "country").collect()

    expected = [
        (1, "Adult", "USA"),
        (2, "Young", "CANADA"),
        (3, "Senior", "UK"),
        (4, "Young", "GERMANY")
    ]

    # Assert for transformed values
    for row, exp in zip(result, expected):
        assert row.id == exp[0]
        assert row.age_group == exp[1]
        assert row.country == exp[2]

    # Check if created_at is converted to DateType
    assert str(result_df.schema["created_at"].dataType) == "DateType"

    # Check record_inserted_at column exists
    assert "record_inserted_at" in result_df.columns
